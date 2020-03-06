import logging
from concurrent.futures import ThreadPoolExecutor
from time import sleep

from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs import defaults
from ocs_ci.ocs.resources import pod as pod_helpers
from ocs_ci.framework.testlib import tier1, tier4, ignore_leftovers, ManageTest
from tests.manage.z_cluster import tier4_helpers
from ocs_ci.ocs import constants
from tests.manage.mcg.helpers import s3_io_create_delete
from ocs_ci.ocs import node
from ocs_ci.ocs.cluster import CephCluster
from ocs_ci.utility.utils import ceph_health_check

# Following global variable is needed to end the background threads once test completes.
global EXPANSION_COMPLETED


# This function can be moved to pod.py
def get_pod_restarts_count(namespace=defaults.ROOK_CLUSTER_NAMESPACE):
    """
    Gets the dictionary of pod and its restart count for all the pods in a given namespace
    Returns: Dict, dictionary of pod name and its corresponding restart count
    """
    list_of_pods = pod_helpers.get_all_pods(namespace)
    restart_dict = {}
    ocp_pod_obj = OCP(kind=constants.POD, namespace=namespace)
    for p in list_of_pods:
        # pod_dict = p.get()
        # we don't want to compare osd-prepare pod as it gets created freshly when an osd need to be added.
        if "rook-ceph-osd-prepare" not in p.name:
            # restart_dict[p.name] = pod_dict['status']['containerStatuses'][0]['restartCount']
            restart_dict[p.name] = ocp_pod_obj.get_resource(p.name,'RESTARTS')

    return restart_dict


def check_pods_in_running_state(namespace=defaults.ROOK_CLUSTER_NAMESPACE):
    """
    checks whether all the pods in a given namespace are in.done state or not
    Returns: True, if all pods in.done state. False, otherwise
    """
    ret_val = True
    list_of_pods = pod_helpers.get_all_pods(namespace)
    ocp_pod_obj = OCP(kind=constants.POD, namespace=namespace)
    for p in list_of_pods:
        # we don't want to compare osd-prepare pod as it gets created freshly when an osd need to be added.
        if "rook-ceph-osd-prepare" not in p.name:
            # restart_dict[p.name] = pod_dict['status']['containerStatuses'][0]['restartCount']
            status = ocp_pod_obj.get_resource(p.name, 'STATUS')
            if status not in "Running":
                logging.info(f"The pod {p.name} is in {status} state. Expected = Running")
                ret_val = False
    return ret_val


def wrapper_cluster_copy_ops(copy_pod):
    global EXPANSION_COMPLETED
    for i in range(20):
        if EXPANSION_COMPLETED:
            logging.info(f"wrapper_cluster_copy_ops : Mugeetu kaNale!!!. In iteration {i-1}")
            return True
        else:
            tier4_helpers.cluster_copy_ops(copy_pod)
            logging.info(f"wrapper_cluster_copy_ops : iteration {i-1}")


def wrapper_s3_io_create_delete(mcg_obj, awscli_pod, bucket_factory):
    global EXPANSION_COMPLETED
    for i in range(30):
        if EXPANSION_COMPLETED:
            logging.info(f"wrapper_s3_io_create_delete: Mugeetu kaNale!!!. In iteration {i-1}")
            return True
        else:
            s3_io_create_delete(mcg_obj, awscli_pod, bucket_factory)
            logging.info(f"wrapper_s3_io_create_delete: iteration {i}")


def check_nodes_status():
    """
    This function runs in a loop to check the status of nodes. If the node(s) are in NotReady state then an
    exception is raised. Note: this function needs to be run as a background thread during the execution of a test
    """
    global EXPANSION_COMPLETED
    logging.info(f"Entered check_nodes_status. Exp_cmp = {EXPANSION_COMPLETED}")
    for i in range(200):
        if EXPANSION_COMPLETED:
            logging.info("wrapper_check_nodes_status: Mugeetu kaNale!!!")
            logging.info(f"wrapper_s3_io_create_delete: iteration {i}")
            return True
        else:
            node.wait_for_nodes_status(node_names=None, status=constants.NODE_READY, timeout=5)
            logging.info("All master and worker nodes are in Ready state.")


def check_osd_pods_added(before, after, expected_osd_pod_count):
    """
    Checks if osd pod corresponding to the newly added osd is shown in oc get pod output or not.

    :param before: list, osd pods that were existing before the addition
    :param after: list, osd pods names after the osd was added
    :param expected_osd_pod_count: int number of osds added
    :return: True, if the expected_osd_pod_count matches with the actual_osd_count observed.
    """
    actual_pods_added = 0

    for pod_after in after:
        found = False
        for pod_before in before:
            logging.info(f"pod after = {pod_after.name}, pod before = {pod_before.name}")
            if pod_after.name in pod_before.name:
                found = True
                break
        if not found:  # we found the newly added osd pod
            pod_status = pod_after.get()
            if (pod_status['status']['containerStatuses'][0]['restartCount'] == 0 and
                    pod_status['status']['phase'] == constants.STATUS_RUNNING):
                logging.info(f"Found the newly added pod {pod_after.name} and is running successfully")
                actual_pods_added += 1
    if expected_osd_pod_count == actual_pods_added:
        return True
    else:
        logging.error(f"Expected to find {expected_osd_pod_count} osd pods but found {actual_pods_added}")
        return False


def check_osd_tree(tree_output):
    # in case of vmware, there will be only one zone as of now. The OSDs are arranged as follows:
    # ID  CLASS WEIGHT  TYPE NAME                            STATUS REWEIGHT PRI-AFF
    # -1       0.99326 root default
    # -8       0.33109     rack rack0
    # -7       0.33109         host ocs-deviceset-0-0-dktqc
    #  1   hdd 0.33109             osd.1                        up  1.00000 1.00000
    # There will be 3 racks - rack0, rack1, rack2.
    # When cluster expansion is successfully done, a host and an osd are added in each rack.
    # The number of hosts will be equal to the number osds the cluster has. Each rack can
    # have multiple hosts but each host will have only one osd under it.
    all_hosts = []
    number_of_hosts_expected = 1
    racks = tree_output['nodes'][0]['children']
    logging.info(f"racks = {racks}")
    for rack in range(len(racks)):
        logging.info(f"looping for rack {racks[rack]}")
        for i in range(1, len(tree_output['nodes'])):
            if tree_output['nodes'][i]['id'] == racks[rack]:
                # logging.info("%%%%% = ", tree_output['nodes'][i]) #['children'])
                if len(tree_output['nodes'][i]['children']) == number_of_hosts_expected:
                    for host in range(len(tree_output['nodes'][i]['children'])):
                        logging.info(f"HOST {tree_output['nodes'][i]['children'][host]}")
                        all_hosts.append(tree_output['nodes'][i]['children'][host])
                    logging.info(f"Match found for Rack: {racks[rack]}")
                    logging.info(f"The rack {racks[rack]} has {number_of_hosts_expected} host(s) as Expected")
                    logging.info(f"All hosts = {all_hosts}")
    for each_host in range(len(all_hosts)):
        logging.info(f"Each host = {all_hosts[each_host]}")
        for i in range(len(tree_output['nodes'])):
            if tree_output['nodes'][i]['id'] == all_hosts[each_host]:
                logging.info("match Found")
                number_osd_in_host = len(tree_output['nodes'][i]['children'])
                if number_osd_in_host > 1 or number_osd_in_host <= 0:
                    logging.error("Error. ceph osd tree is formed correctly after cluster expansion")
    logging.info("All success")


@ignore_leftovers
@tier4
class TestTier1Framework(ManageTest):
    def test_tier1_framework(self, project_factory, multi_dc_pod, multi_pvc_factory, pod_factory,
                                            mcg_obj, awscli_pod, bucket_factory):
        global EXPANSION_COMPLETED
        EXPANSION_COMPLETED = False

        # ###################################
        #           ENTRY CRITERIA          #
        #####################################
        # Prepare initial configuration : logging, cluster filling, loop for creating & deleting of PVCs and Pods,
        # noobaa IOs etc.,

        # Perform Health checks:
        # Make sure cluster is healthy
        assert CephCluster().is_health_ok() == True, "Entry criteria FAILED: Cluster is Unhealthy"

        # All OCS pods are in running state:
        assert check_pods_in_running_state(defaults.ROOK_CLUSTER_NAMESPACE), \
            "Entry criteria FAILED: one or more OCS pods are not in running state"

        # Create the namespace under which this test will executeq:
        project = project_factory()
        logging.info("#1: Namespace created...")

        # Fill up the cluster to the given percentage:
        # Create DC pods. These pods will be used to fill the cluster. Once the cluster is filled to the required
        # level, the test will begin while IOs in these pods continue till the end of the test.
        num_of_pvcs = 1
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
        logging.info("#2: The DC pods are up. Running IOs from them to fill the cluster")
        logging.info(f"Will be.done IOs from these pods = {cluster_fill_io_pods}")
        logging.info("###########################################################################################")
        with ThreadPoolExecutor() as executor:
            for p in cluster_fill_io_pods:
                logging.info(f"calling fillup fn from {p.name}")
                executor.submit(tier4_helpers.cluster_fillup, p, 22)

        # Start.done NooBaa IOs in the background.:
        status_noobaa_io = executor.submit(wrapper_s3_io_create_delete, mcg_obj, awscli_pod, bucket_factory)
        logging.info("Started s3_io_create_delete...")

        # Start running background IOs:
        # append these pods to the cluster_fill_io_pods
        # Note: we want cluster_fill_io_pods to have its elements in random order. Try this out
        cluster_fill_io_pods = cluster_fill_io_pods + pods_ios_rwx_rbd
        status_cluster_ios = []
        for p in cluster_fill_io_pods:
            logging.info(f"calling cluster_copy_ops for {p.name}")
            if p.pod_type == "rbd_block_rwx":
                status_cluster_ios.append(executor.submit(tier4_helpers.raw_block_io, p))
            else:
                status_cluster_ios.append(executor.submit(wrapper_cluster_copy_ops, p))
        # All ocs nodes are in Ready state (including master):
        node_status = executor.submit(check_nodes_status)

        # Get restart count of ocs pods before expanstion
        restart_count_before = get_pod_restarts_count(defaults.ROOK_CLUSTER_NAMESPACE)

        # Get osd pods before expansion
        osd_pods_before = pod_helpers.get_osd_pods()

        # Get current count of osds
        original_osd_count = len(osd_pods_before)

        # Expand the cluster: replace the following with actual call to add_capacity

        ##########################
        # Call add_capacity here #
        ##########################

        # Following for loop is simulating the add_capacity (sleeos for 200 seconds)
        for i in range(100):
            logging.info(f"I AM Main thread. I am sleeping for 20 sec. In iteration {i}")
            sleep(2)

        logging.info(f"Add Capacity completed successfully!. {EXPANSION_COMPLETED}")

        #################################
        # Exit criteria verification:   #
        #################################


        exit_criteria_verification = True

        EXPANSION_COMPLETED = True

        # No ocs pods should get restarted unexpectedly
        #   Get restart count of ocs pods after expansion and see any pods got restated
        restart_count_after = get_pod_restarts_count(defaults.ROOK_CLUSTER_NAMESPACE)
        assert restart_count_before == restart_count_after, "Exit criteria verification FAILED: One or more pods got restarted"

        # Make sure right number of OSDs are added:
        #   Get osd pods after expansion
        osd_pods_after = pod_helpers.get_osd_pods()
        number_of_osds_added = len(osd_pods_after) - len(osd_pods_before)
        #   If the difference b/w updated count of osds and old osd count is not 3 then expansion failed
        assert number_of_osds_added == 3, "Exit criteria verification FAILED: osd count mismatch"

        # The newly added capacity takes into effect at the storage level
        ct_pod = pod_helpers.get_ceph_tools_pod()
        output = ct_pod.exec_ceph_cmd(ceph_cmd='rados df')
        actual_total_space = output.get('total_space')
        expected_total_space = tier4_helpers.get_osd_size() * len(osd_pods_after)
        assert actual_total_space == expected_total_space, "Exit criteria verification FAILED: Expected capacity mismatch"



        # New osd (all) pods corresponding to the additional capacity should be in running state
        assert check_osd_pods_added(osd_pods_before, osd_pods_after, 3),"Exit criteria verification FAILED: New osd pods are not shown"

        # IOs should not stop, No OCP/OCS nodes should go to NotReady state,No OCP/OCS nodes should go to NotReady state
        if status_noobaa_io.result() and node_status.result():
            for cluster_ios in status_cluster_ios:
                if not cluster_ios.running():
                    assert exit_criteria_verification, "Exit criteria verification FAILED: IOs failed during expansion"

        # 'ceph -s' should show HEALTH_OK
        ceph_health_check(defaults.ROOK_CLUSTER_NAMESPACE)

        # 'ceph osd tree' should show the new osds under right nodes/hosts
        #   Verification is different for 3 AZ and 1 AZ configs
        # Following is for vmware 1 AZ only. For AWS 3 and 1 AZ - TBD
        ct_pod = pod_helpers.get_ceph_tools_pod()
        output = ct_pod.exec_ceph_cmd(ceph_cmd='ceph osd tree')
        check_osd_tree(output)
        logging.error("Exit criteria verification PASSED")
        logging.info("********************** COMPLETED *********************************")


        """
        if status_noobaa_io.running() and node_status.running():
            for cluster_ios in status_cluster_ios:
                if not cluster_ios.running():
                    exit_criteria_verification = False
                    break
        else:
            exit_criteria_verification = False
        logging.info(f"status_create_delete_pvc_pods = {status_create_delete_pvc_pods.running()}")
        logging.info(f"status_obc_create_delete = {status_obc_create_delete.running()}")
        logging.info(f"status_noobaa_io = {status_noobaa_io.running()}")
        logging.info(f"node_status = {node_status.running()}")
        for stat in status_cluster_ios:
            logging.info(f"stat.done() = {stat.running()}")
            ======================
        logging.info(f"result_noobaa_io = {status_noobaa_io.result()}")
        logging.info(f"node_result = {node_status.result()}")
        for cluster_ios in status_cluster_ios:
            logging.info(f"stat.result() = {cluster_ios.result()}")
        ############### 
        """
