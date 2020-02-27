import logging
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs import defaults
from ocs_ci.ocs.resources import pod
from ocs_ci.utility import deployment_openshift_logging as ocp_logging_obj
from ocs_ci.framework.testlib import tier1, tier4, ignore_leftovers, ManageTest
from tests.manage.z_cluster import tier4_helpers
from concurrent.futures import ThreadPoolExecutor, as_completed
from ocs_ci.ocs import constants
from tests.manage.z_cluster.cluster_expansion.create_delete_pvc_parallel import test_create_delete_pvcs
from tests.manage.mcg.helpers import s3_io_create_delete
from ocs_ci.utility import deployment_openshift_logging as ocp_logging_obj

import pytest
from ocs_ci.framework import config
from multiprocessing import Process
import time
from concurrent.futures import ThreadPoolExecutor

from tests import helpers
from ocs_ci.ocs.cluster import CephCluster
#from tests.manage.z_cluster.cluster_expansion.create_delete_pvc_parallel import test_create_delete_pvcs
# from tests.manage.mcg.test_write_to_bucket import TestBucketIO
logger = logging.getLogger(__name__)


# This function can be moved to pod.py
def get_pod_restarts_count(namespace=defaults.ROOK_CLUSTER_NAMESPACE):
    """
    Gets the dictionary of pod and its restart count for all the pods in a given namespace
    Returns: Dict, dictionary of pod name and its corresponding restart count
    """
    list_of_pods = pod.get_all_pods(namespace)
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
    checks whether all the pods in a given namespace are in running state or not
    Returns: True, if all pods in running state. False, otherwise
    """
    ret_val = True
    list_of_pods = pod.get_all_pods(namespace)
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


def check_ocp_workloads():

    # get a count of the restarts for each of the pods in the monitoring
    restarts_count = get_pod_restarts_count(defaults.OCS_MONITORING_NAMESPACE)

    # check if all pods in monitoring namespace are in Running state or not
    assert check_pods_in_running_state(defaults.OCS_MONITORING_NAMESPACE)
    logging.info("Monitoring Pods are healthy")

    # check if the health of the logging is good or bad
    ocp_logging_obj.check_health_of_clusterlogging()
    logging.info("Logging is healthy")

    # TO DO: Check Registry

    return restarts_count


def entry_criteria_check_configure_ocp_workloads():
    # create the logging infra if not created already
    if not pod.get_all_pods(constants.OPENSHIFT_LOGGING_NAMESPACE):
        logging.info("### LOGGING is NOT Configured ###")
        ocp_logging_obj.create_logging_infra()

    return check_ocp_workloads()


def exit_criteria_check_ocp_workloads():
    return check_ocp_workloads()


def junk1():
    logging.info("in junk1")
    executor = ThreadPoolExecutor(max_workers=32)
    executor.submit(junk2)
    from time import sleep
    for i in range(50):
        logging.info(f"iteration = {i}")
        sleep(4)

def junk2():
    logging.info("in junk2")

@ignore_leftovers
@tier4
class TestTier4Framework(ManageTest):
    def test_tier4_framework(self, project_factory, multi_dc_pod, multi_pvc_factory, pod_factory,
                                            mcg_obj, awscli_pod, bucket_factory):

        # ############# ENTRY CRITERIA ##############
        # Prepare initial configuration : cluster filling, loop for creating & deleting of PVCs and Pods, noobaa IOs

        # #1. Create the namespace under which this test will execute
        project = project_factory()
        logging.info("#1: Namespace created...")

        """
        # #2. Fill up the cluster to the given percentage
        # get the number of pods and iterations to run to fill the cluster for a given %age fill factor
        cluster_pods_to_create, cluster_fill_iterations = tier4_helpers.cluster_fill_get_pod_iter("37")

        num_of_pvcs = int(cluster_pods_to_create/3)
        logging.info(f"1. number of pvcs to create = {num_of_pvcs}")

        # Create DC pods. These pods will be used to fill the cluster. Once the cluster is filled to the required
        # level, the test will begin while IOs in these pods continue till the end of the test.
        rwo_rbd_pods = multi_dc_pod(num_of_pvcs=num_of_pvcs, pvc_size=200,
                                    project=project, access_mode="RWO", pool_type='rbd')
        rwo_cephfs_pods = multi_dc_pod(num_of_pvcs=num_of_pvcs, pvc_size=200,
                                       project=project, access_mode="RWO", pool_type='cephfs')
        rwx_cephfs_pods = multi_dc_pod(num_of_pvcs=num_of_pvcs, pvc_size=200,#
                                       project=project, access_mode="RWX", pool_type='cephfs')

        pods = rwo_rbd_pods + rwo_cephfs_pods + rwx_cephfs_pods
        logging.info("#2: The DC pods are up. Running IOs from them to fill the cluster")
        logging.info(f"Will be running IOs from these pods = {pods}")
        logging.info("###########################################################################################")

        jobs = []
        with ThreadPoolExecutor() as executor:
            for p in pods:
                logging.info(f"calling fillup fn from {p.name}")
                jobs.append(executor.submit(tier4_helpers.cluster_fillup, p, cluster_fill_iterations))

        """
        # #3. Check for entry criteria on Logging, monitoring and registry
        # monitoring_pods_restarts_count = entry_criteria_check_configure_ocp_workloads()

        # #4. Start running the operations like PVC and Pod create and delete in background
        executor = ThreadPoolExecutor(max_workers=32)
        """
        status_create_delete_pvc_pods = executor.submit(test_create_delete_pvcs,multi_pvc_factory, pod_factory, project)
        from time import sleep
        while not (status_create_delete_pvc_pods.done()):
            sleep(15)
            logging.info("############## Not completed. sleeping....")
        """

        """
        # #5. Start running NooBaa IOs in the background.

        status_noobaa_io = executor.submit(s3_io_create_delete, mcg_obj, awscli_pod, bucket_factory)
        while not (status_noobaa_io.done()):
            sleep(15)
            logging.info("############## Not completed. sleeping....")
        """

        # #5a. Start running copy operations from some pods
        # create n pods - rwo, rwx, rwx-block rbd, cephfs
        num_pvcs = 1
        pods_ios_rwx_rbd = multi_dc_pod(num_of_pvcs=num_pvcs, pvc_size=20,
                                       project=project, access_mode="RWX-BLK", pool_type='rbd')
        pods_ios_rwo_rbd = multi_dc_pod(num_of_pvcs=num_pvcs, pvc_size=20,
                                        project=project, access_mode="RWO", pool_type='rbd')
        pods_ios_rwo_cephfs = multi_dc_pod(num_of_pvcs=num_pvcs, pvc_size=20,
                                           project=project, access_mode="RWO", pool_type='cephfs')
        pods_ios_rwx_cephfs = multi_dc_pod(num_of_pvcs=num_pvcs, pvc_size=20,
                                           project=project, access_mode="RWX", pool_type='cephfs')
        pods = pods_ios_rwx_rbd + pods_ios_rwo_rbd # + pods_ios_rwo_cephfs + pods_ios_rwx_cephfs
        jobs = []
        jobs_rbd_rwx = []
        with ThreadPoolExecutor(max_workers=(num_pvcs * 3)) as executor:
            for p in pods:
                logging.info(f"calling cluster_copy_ops for {p.name}")
                if p.pod_type == "rbd_block_rwx":
                    jobs_rbd_rwx.append(executor.submit(tier4_helpers.raw_block_io, p))
                else:
                    jobs.append(executor.submit(tier4_helpers.cluster_copy_ops, p, iterations=1))

        from time import sleep
        for j in jobs:
            while not (j.done()):
                logging.info(f"### job {j} not complete. sleeping....")
                sleep(15)
            logging.info(f"#### Result of the job {j} is = {j.done()}")
            if not j.done():
                logging.error(f"#### Data integrity check failure. Test failed.")
                exit(1)
        """
        with ThreadPoolExecutor(max_workers=num_pvcs) as executor:
            for p in pods_ios_rwx_rbd:
                logging.info(f"calling fillup fn from {p.name}")
                jobs.append(executor.submit(tier4_helpers.raw_block_io, p))
        """

        # #5b. Start running pgsql
        # #6. Make sure PGs are in active+clean state
        # #9. Cluster has lesser than 3 osds per node.
        # #10. All OCS pods are in running state
        # #11. All nodes are in Ready state (including master)
        # #7. Background operations including IOs are running successfully
        # if Not (status_create_delete_pvc_pods.done() and status_noobaa_io.done()):
        #     logging.error("ERROR: Background operations including IOs are NOT running")
        # #8. All the existing OSDs are of size 2TiB(from 4.3 this is configurable)
        # #12. Expand the cluster


