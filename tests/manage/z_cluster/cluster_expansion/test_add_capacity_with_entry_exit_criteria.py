import logging
from concurrent.futures import ThreadPoolExecutor
import pytest

from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs import defaults
from ocs_ci.ocs.resources import pod as pod_helpers
from ocs_ci.framework.testlib import tier1, ignore_leftovers, ManageTest
from tests.manage.z_cluster import tier4_helpers
from ocs_ci.ocs import constants
from tests.manage.mcg.helpers import s3_io_create_delete
from ocs_ci.ocs import node
from ocs_ci.ocs.cluster import CephCluster
from ocs_ci.ocs import cluster as cluster_helpers
from ocs_ci.ocs.resources import storage_cluster
from ocs_ci.utility.utils import ceph_health_check
from ocs_ci.framework import config
from ocs_ci.utility.aws import AWS

# Add capacity needs to happen with background operations/IOs in progress. In order to do this we use threads. Each
# thread runs a function responsible to do the background operations/IOs while add capacity operation is in progress.
# When the expansion completes, the threads will keep running even all verification steps are over. ThreadPoolExecutor
# does not provide function to kill the threads (it's not a good idea to kill the threads abruptly).
# So in order to that gracefully, we are using the below mentioned global variable which is checked for True before
# starting each iteration of background operation by each thread. If variable is set to False, then it means the
# expansion is completed and the iterations are stopped and the function returns thereby ending the thread.
global EXPANSION_COMPLETED

# TO DO: replace/remove this with actual workloads like couchbase, amq and pgsql
def wrapper_cluster_copy_ops(copy_pod, iterations=1):
    global EXPANSION_COMPLETED
    for i in range(iterations):
        if EXPANSION_COMPLETED:
            logging.info(f"wrapper_cluster_copy_ops : Done with execution. Stopping the thread. In iteration {i-1}")
            return True
        else:
            assert tier4_helpers.cluster_copy_ops(copy_pod), "Data integrity check FAILED"
            logging.info(f"wrapper_cluster_copy_ops : iteration {i-1}")


def wrapper_s3_io_create_delete(mcg_obj, awscli_pod, bucket_factory, iterations=1):
    global EXPANSION_COMPLETED
    for i in range(iterations):
        if EXPANSION_COMPLETED:
            logging.info(f"wrapper_s3_io_create_delete: Done with execution. Stopping the thread. In iteration {i-1}")
            return True
        else:
            s3_io_create_delete(mcg_obj, awscli_pod, bucket_factory)
            logging.info(f"wrapper_s3_io_create_delete: iteration {i}")


def wrapper_raw_block_ios(pod, iterations):
    global EXPANSION_COMPLETED
    for i in range(iterations):
        if EXPANSION_COMPLETED:
            logging.info(f"wrapper_raw_block_ios: Done with execution. Stopping the thread. In iteration {i-1}")
            return True
        else:
            tier4_helpers.raw_block_io(pod, '100G')
            logging.info(f"wrapper_raw_block_ios: iteration {i}")


def check_nodes_status():
    """
    This function runs in a loop to check the status of nodes. If the node(s) are in NotReady state then an
    exception is raised. Note: this function needs to be run as a background thread during the execution of a test
    """
    global EXPANSION_COMPLETED
    logging.info(f"Entered check_nodes_status. Exp_cmp = {EXPANSION_COMPLETED}")
    for i in range(200):
        if EXPANSION_COMPLETED:
            logging.info(f"check_nodes_status : Done with execution. Stopping the thread. In iteration {i-1}")
            return True
        else:
            node.wait_for_nodes_status(node_names=None, status=constants.NODE_READY, timeout=5)
            logging.info("All master and worker nodes are in Ready state.")


@pytest.mark.parametrize(
    argnames=["percent_to_fill"],
    argvalues=[
        pytest.param(
            *[1],
            marks=pytest.mark.polarion_id("OCS-2131")
        ),
     ]
)


@ignore_leftovers
@tier1
class TestTier1Framework(ManageTest):
    def test_tier1_framework(self, project_factory, multi_dc_pod, multi_pvc_factory, pod_factory,
                                            mcg_obj, awscli_pod, bucket_factory, percent_to_fill):
        global EXPANSION_COMPLETED
        EXPANSION_COMPLETED = False

        # ###################################
        #           ENTRY CRITERIA          #
        #####################################
        # Prepare initial configuration : logging, cluster filling, loop for creating & deleting of PVCs and Pods,
        # noobaa IOs etc.,

        # Perform Health checks:
        # Make sure cluster is healthy
        assert CephCluster().is_health_ok(), "Entry criteria FAILED: Cluster is Unhealthy"

        # All OCS pods are in running state:
        assert pod_helpers.check_pods_in_running_state(), \
            "Entry criteria FAILED: one or more OCS pods are not in running state"#

        # Create the namespace under which this test will executeq:
        project = project_factory()

        num_of_pvcs = 9 # total pvc created will be 'num_of_pvcs' * 4 types of pvcs(rbd-rwo,rwx & cephfs-rwo,rwx)

        rwo_rbd_pods = multi_dc_pod(num_of_pvcs=num_of_pvcs, pvc_size=175,
                                    project=project, access_mode="RWO", pool_type='rbd')
        rwo_cephfs_pods = multi_dc_pod(num_of_pvcs=num_of_pvcs, pvc_size=175,
                                       project=project, access_mode="RWO", pool_type='cephfs')
        rwx_cephfs_pods = multi_dc_pod(num_of_pvcs=num_of_pvcs, pvc_size=175,
                                       project=project, access_mode="RWX", pool_type='cephfs')
        # Create rwx-rbd pods
        pods_ios_rwx_rbd = multi_dc_pod(num_of_pvcs=num_of_pvcs, pvc_size=175,
                                        project=project, access_mode="RWX-BLK", pool_type='rbd')
        cluster_fill_io_pods = rwo_rbd_pods + rwo_cephfs_pods + rwx_cephfs_pods
        #cluster_fill_io_pods = rwo_rbd_pods # + rwo_cephfs_pods + rwx_cephfs_pods
        logging.info("#2: The DC pods are up. Running IOs from them to fill the cluster")
        logging.info(f"Will running IOs from these pods = {cluster_fill_io_pods}")
        logging.info("###########################################################################################")

        tier4_helpers.cluster_filler(cluster_fill_io_pods, percent_to_fill)
        logging.info("###################### $$$$$$$$$$$$$$$$$$$$$$$$$")

        # create separate threadpool for running IOs in the background
        executor_run_bg_ios_ops = ThreadPoolExecutor()

        # Start NooBaa IOs in the background.:
        status_noobaa_io = executor_run_bg_ios_ops.submit(wrapper_s3_io_create_delete, mcg_obj, awscli_pod, bucket_factory,30)
        logging.info("Started s3_io_create_delete...")

        # Start running background IOs:
        # append these pods to the cluster_fill_io_pods
        # Note: we want cluster_fill_io_pods to have its elements in random order. Try this out
        cluster_fill_io_pods = cluster_fill_io_pods + pods_ios_rwx_rbd
        status_cluster_ios = []
        for p in cluster_fill_io_pods:
            logging.info(f"calling cluster_copy_ops for {p.name}")
            if p.pod_type == "rbd_block_rwx":
                status_cluster_ios.append(executor_run_bg_ios_ops.submit(wrapper_raw_block_ios, p, 100))
            else:
                status_cluster_ios.append(executor_run_bg_ios_ops.submit(wrapper_cluster_copy_ops, p, 50))
        # All ocs nodes are in Ready state (including master):
        node_status = executor_run_bg_ios_ops.submit(check_nodes_status)

        # Get restart count of ocs pods before expanstion
        restart_count_before = pod_helpers.get_pod_restarts_count(defaults.ROOK_CLUSTER_NAMESPACE)

        # Get osd pods before expansion
        osd_pods_before = pod_helpers.get_osd_pods()

        logging.info("#################################################### Calling add_capacity $$$$$$$$$$")
        #####################
        # Call add_capacity #
        #####################
        osd_size = storage_cluster.get_osd_size()
        result = storage_cluster.add_capacity(osd_size)
        pod = OCP(
            kind=constants.POD, namespace=config.ENV_DATA['cluster_namespace']
        )
        pod.wait_for_resource(
            timeout=1200,
            condition=constants.STATUS_RUNNING,
            selector='app=rook-ceph-osd',
            resource_count=result * 3
        )

        #################################
        # Exit criteria verification:   #
        #################################
        EXPANSION_COMPLETED = True

        # 'ceph -s' should show HEALTH_OK
        # assert ceph_health_check(defaults.ROOK_CLUSTER_NAMESPACE), \
        #     "Exit criteria verification FAILED: Cluster unhealthy"

        # No ocs pods should get restarted unexpectedly
        #   Get restart count of ocs pods after expansion and see any pods got restated
        restart_count_after = pod_helpers.get_pod_restarts_count(defaults.ROOK_CLUSTER_NAMESPACE)
        if restart_count_before == restart_count_after:
            logging.info("Exit criteria verification FAILED: One or more pods got restarted")

        # Make sure right number of OSDs are added:
        #   Get osd pods after expansion
        osd_pods_after = pod_helpers.get_osd_pods()
        number_of_osds_added = len(osd_pods_after) - len(osd_pods_before)
        #   If the difference b/w updated count of osds and old osd count is not 3 then expansion failed
        if number_of_osds_added == 3:
            logging.info("Exit criteria verification FAILED: osd count mismatch")

        # The newly added capacity takes into effect at the storage level
        ct_pod = pod_helpers.get_ceph_tools_pod()
        output = ct_pod.exec_ceph_cmd(ceph_cmd='rados df')
        actual_total_space = output.get('total_space')
        expected_total_space = storage_cluster.get_osd_size() * len(osd_pods_after)
        if actual_total_space == expected_total_space:
            logging.info("Exit criteria verification FAILED: Expected capacity mismatch")

        # New osd (all) pods corresponding to the additional capacity should be in running state
        if not storage_cluster.check_osd_pods_added(osd_pods_before, osd_pods_after, 3):
            logging.info("Exit criteria verification FAILED: New osd pods are not shown")

        # IOs should not stop, No OCP/OCS nodes should go to NotReady state,No OCP/OCS nodes should
        # go to NotReady state
        if status_noobaa_io.result() and node_status.result():
            for cluster_ios in status_cluster_ios:
                if not cluster_ios.running():
                    logging.info("Exit criteria verification FAILED: IOs failed during expansion")

        # 'ceph osd tree' should show the new osds under right nodes/hosts
        #   Verification is different for 3 AZ and 1 AZ configs
        ct_pod = pod_helpers.get_ceph_tools_pod()
        tree_output = ct_pod.exec_ceph_cmd(ceph_cmd='ceph osd tree')
        if config.ENV_DATA['platform'] == 'vsphere':
            if not cluster_helpers.check_osd_tree_1az_vmware(tree_output, 6):  #len(osd_pods_after))::
                 logging.info("Exit criteria verification FAILED: Incorrect ceph osd tree formation found")

        aws_number_of_zones = 3
        if config.ENV_DATA['platform'] == 'AWS':
            # parse the osd tree. if it contains a node 'rack' then it's a AWS_1AZ cluster. Else, 3 AWS_3AZ cluster
            for i in range(len(tree_output['nodes'])):
                if tree_output['nodes'][i]['name'] in "rack":
                    aws_number_of_zones = 1
            if aws_number_of_zones == 1:
                if not cluster_helpers.check_osd_tree_1az_aws(output, len(osd_pods_after)):
                    logging.error("Exit criteria verification FAILED: Incorrect ceph osd tree formation found")
            else:
                if not cluster_helpers.check_osd_tree_3az_aws(output, len(osd_pods_after)):
                    logging.error("Exit criteria verification FAILED: Incorrect ceph osd tree formation found")

        # Make sure new pvcs and pods can be created and IOs can be run from the pods
        num_of_pvcs = 1
        rwo_rbd_pods = multi_dc_pod(num_of_pvcs=num_of_pvcs, pvc_size=5,
                                    project=project, access_mode="RWO", pool_type='rbd')

        rwo_cephfs_pods = multi_dc_pod(num_of_pvcs=num_of_pvcs, pvc_size=5,
                                       project=project, access_mode="RWO", pool_type='cephfs')
        rwx_cephfs_pods = multi_dc_pod(num_of_pvcs=num_of_pvcs, pvc_size=5,
                                       project=project, access_mode="RWX", pool_type='cephfs')
        # Create rwx-rbd pods
        pods_ios_rwx_rbd = multi_dc_pod(num_of_pvcs=num_of_pvcs, pvc_size=5,
                                        project=project, access_mode="RWX-BLK", pool_type='rbd')
        cluster_io_pods = rwo_rbd_pods + rwo_cephfs_pods + rwx_cephfs_pods + pods_ios_rwx_rbd
        for p in cluster_io_pods:
            if p.pod_type == "rbd_block_rwx":
                tier4_helpers.raw_block_io(p, '2G')
            else:
                p.run_io('fs', '2G')
        logging.error("Exit criteria verification PASSED")
        logging.info("********************** COMPLETED *********************************")



def junk():

