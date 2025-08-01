import random
import time

import pytest
import logging

from datetime import datetime, timezone

from ocs_ci.framework.pytest_customization.marks import (
    polarion_id,
    stretchcluster_required,
    turquoise_squad,
    tier2,
)
from ocs_ci.helpers.cnv_helpers import cal_md5sum_vm
from ocs_ci.helpers.helpers import modify_deployment_replica_count
from ocs_ci.helpers.stretchcluster_helper import recover_workload_pods_post_recovery
from ocs_ci.ocs.exceptions import UnexpectedBehaviour
from ocs_ci.ocs.resources.pod import (
    get_not_running_pods,
    wait_for_pods_to_be_in_statuses,
    get_deployment_name,
    wait_for_pods_by_label_count,
    get_all_pods,
    get_pod_node,
)
from ocs_ci.ocs.resources.pvc import get_pvc_objs
from ocs_ci.ocs.resources.stretchcluster import StretchCluster
from ocs_ci.ocs import constants

logger = logging.getLogger(__name__)

CNV_WORKLOAD_NAMESPACE = "namespace-cnv-workload"


@pytest.fixture(scope="class")
def setup_logwriter_workloads(
    request,
    setup_logwriter_cephfs_workload_class,
    setup_logwriter_rbd_workload_class,
    logreader_workload_class,
):
    sc_obj = StretchCluster()
    # Run the logwriter cephFs and RBD workloads
    (
        sc_obj.cephfs_logwriter_dep,
        sc_obj.cephfs_logreader_job,
    ) = setup_logwriter_cephfs_workload_class(read_duration=0)
    sc_obj.rbd_logwriter_sts = setup_logwriter_rbd_workload_class(zone_aware=False)

    # Generate 5 minutes worth of logs before inducing the netsplit
    logger.info("Generating 2 mins worth of log")
    time.sleep(120)

    sc_obj.get_logwriter_reader_pods(label=constants.LOGWRITER_CEPHFS_LABEL)
    sc_obj.get_logwriter_reader_pods(label=constants.LOGREADER_CEPHFS_LABEL)
    sc_obj.get_logwriter_reader_pods(
        label=constants.LOGWRITER_RBD_LABEL, exp_num_replicas=2
    )
    logger.info("All the workloads pods are successfully up and running")

    start_time = datetime.now(timezone.utc)
    sc_obj.get_logfile_map(label=constants.LOGWRITER_CEPHFS_LABEL)
    sc_obj.get_logfile_map(label=constants.LOGWRITER_RBD_LABEL)

    def finalizer():
        """
        Check for data loss, data corruption at the end of the tests

        """
        end_time = datetime.now(timezone.utc)

        try:
            sc_obj.get_logwriter_reader_pods(label=constants.LOGWRITER_CEPHFS_LABEL)
            sc_obj.get_logwriter_reader_pods(
                label=constants.LOGREADER_CEPHFS_LABEL,
                statuses=[constants.STATUS_RUNNING, constants.STATUS_COMPLETED],
            )
            sc_obj.get_logwriter_reader_pods(
                label=constants.LOGWRITER_RBD_LABEL, exp_num_replicas=2
            )
        except UnexpectedBehaviour:

            logger.info(
                "Some app pods are not running, so trying the work-around to make them `Running`"
            )
            pods_not_running = get_not_running_pods(
                namespace=constants.STRETCH_CLUSTER_NAMESPACE
            )
            recover_workload_pods_post_recovery(sc_obj, pods_not_running)

        sc_obj.post_failure_checks(start_time, end_time, wait_for_read_completion=False)
        logger.info("Successfully verified with post failure checks for the workloads")

        sc_obj.cephfs_logreader_job.delete()
        logger.info(sc_obj.cephfs_logreader_pods)
        for pod in sc_obj.cephfs_logreader_pods:
            pod.wait_for_pod_delete(timeout=120)
        logger.info("All old CephFS logreader pods are deleted")

        # check for any data loss
        assert sc_obj.check_for_data_loss(
            constants.LOGWRITER_CEPHFS_LABEL
        ), "[CephFS] Data is lost"
        logger.info("[CephFS] No data loss is seen")
        assert sc_obj.check_for_data_loss(
            constants.LOGWRITER_RBD_LABEL
        ), "[RBD] Data is lost"
        logger.info("[RBD] No data loss is seen")

        # check for data corruption
        pvc = get_pvc_objs(
            pvc_names=[
                sc_obj.cephfs_logwriter_dep.get()["spec"]["template"]["spec"][
                    "volumes"
                ][0]["persistentVolumeClaim"]["claimName"]
            ],
            namespace=constants.STRETCH_CLUSTER_NAMESPACE,
        )[0]
        logreader_workload_class(
            pvc=pvc, logreader_path=constants.LOGWRITER_CEPHFS_READER, duration=5
        )
        sc_obj.get_logwriter_reader_pods(constants.LOGREADER_CEPHFS_LABEL)

        wait_for_pods_to_be_in_statuses(
            expected_statuses=constants.STATUS_COMPLETED,
            pod_names=[pod.name for pod in sc_obj.cephfs_logreader_pods],
            timeout=900,
            namespace=constants.STRETCH_CLUSTER_NAMESPACE,
        )
        logger.info("[CephFS] Logreader job pods have reached 'Completed' state!")

        assert sc_obj.check_for_data_corruption(
            label=constants.LOGREADER_CEPHFS_LABEL
        ), "Data is corrupted for cephFS workloads"
        logger.info("No data corruption is seen in CephFS workloads")

        assert sc_obj.check_for_data_corruption(
            label=constants.LOGWRITER_RBD_LABEL
        ), "Data is corrupted for RBD workloads"
        logger.info("No data corruption is seen in RBD workloads")

    request.addfinalizer(finalizer)


@pytest.fixture(scope="class")
def setup_cnv_workload(request, cnv_workload_class, setup_cnv):

    logger.info("Setting up CNV workload and creating some data")
    vm_obj = cnv_workload_class(
        volume_interface=constants.VM_VOLUME_PVC, namespace=CNV_WORKLOAD_NAMESPACE
    )
    vm_obj.run_ssh_cmd(command="mkdir /test && sudo chmod -R 777 /test")
    vm_obj.run_ssh_cmd(
        command="< /dev/urandom tr -dc 'A-Za-z0-9' | head -c 10485760 > /test/file_1.txt && sync"
    )
    md5sum_before = cal_md5sum_vm(vm_obj, file_path="/test/file_1.txt")
    logger.debug(
        f"This is the file_1.txt content:\n{vm_obj.run_ssh_cmd(command='cat /test/file_1.txt')}"
    )

    def finalizer():

        # check vm data written before the failure for integrity
        logger.info("Waiting for VM SSH connectivity!")
        vm_obj.wait_for_ssh_connectivity()
        md5sum_after = cal_md5sum_vm(vm_obj, file_path="/test/file_1.txt")
        assert (
            md5sum_before == md5sum_after
        ), "Data integrity of the file inside VM is not maintained during the failure"
        logger.info(
            "Data integrity of the file inside VM is maintained during the failure"
        )

        # check if new data can be created
        vm_obj.run_ssh_cmd(
            command="< /dev/urandom tr -dc 'A-Za-z0-9' | head -c 10485760 > /test/file_2.txt"
        )
        logger.info("Successfully created new data inside VM")

        # check if the data can be copied back to local machine
        vm_obj.scp_from_vm(local_path="/tmp", vm_src_path="/test/file_1.txt")
        logger.info("VM data is successfully copied back to local machine")

        # stop the VM
        vm_obj.stop()
        logger.info("Stoped the VM successfully")

    request.addfinalizer(finalizer)


@tier2
@turquoise_squad
@stretchcluster_required
@pytest.mark.usefixtures("setup_cnv_workload")
@pytest.mark.usefixtures("setup_logwriter_workloads")
class TestMonAndOSDFailures:
    """
    Here we test the Mon and OSD failures while CephFS & RBD
    workloads are being run in the background.
        * Run the workloads in the session scoped fixture setup
        * Execute the mon & osd failure tests
        * Verify the IO during the teardown

    """

    @polarion_id("OCS-5059")
    def test_single_mon_failures(self):
        """
        Test single mon failure with cephFS/RBD workloads running in the background

        """
        logger.info("testing single mon failures scenario")
        sc_obj = StretchCluster()

        # get mon-pod of a zone where the cnv workloads
        # are running
        pod_objs = get_all_pods(namespace=CNV_WORKLOAD_NAMESPACE)
        assert len(pod_objs) != 0, "No vmi pod instances are running"
        node_obj = get_pod_node(pod_objs[0])
        mon_pods_in_zone = sc_obj.get_mon_pods_in_a_zone(
            node_obj.get()["metadata"]["labels"][constants.ZONE_LABEL]
        )
        mon_pod_to_fail = random.choice(mon_pods_in_zone).name

        # get the deployment of the mon-pod
        mon_dep = get_deployment_name(mon_pod_to_fail)

        # scale the deployment of mon to 0
        # and wait 10 mins
        logger.info(f"Failing mon by scaling down the deployment {mon_dep}")
        if modify_deployment_replica_count(mon_dep, 0):
            time.sleep(600)

        # scale the deployment back to 1
        logger.info(f"recovering mon {mon_dep} now...")
        modify_deployment_replica_count(mon_dep, 1)
        wait_for_pods_by_label_count(
            label=constants.MON_APP_LABEL, expected_count=5, timeout=300
        )

    @polarion_id("OCS-5060")
    def test_both_mon_failure(self):
        """
        Test both data zone mon failure with cephFS/RBD workloads running in the background

        """
        logger.info("testing mon failures at both the data-zones")
        sc_obj = StretchCluster()
        mon_deps = list()
        expected_mon_count = 5
        for zone in constants.DATA_ZONE_LABELS:
            # get mon-pod of a single zone
            mon_pods_in_zone = sc_obj.get_mon_pods_in_a_zone(zone)
            mon_pod_to_fail = random.choice(mon_pods_in_zone).name

            # get the deployment of the mon-pod
            mon_dep = get_deployment_name(mon_pod_to_fail)

            # scale the deployment of mon to 0
            # and wait 10 mins
            modify_deployment_replica_count(mon_dep, 0)
            logger.info(
                f"Failing mon by scaling down mon deployment {mon_dep} from data-zone {zone}"
            )
            expected_mon_count -= 1
            mon_deps.append(mon_dep)
            wait_for_pods_by_label_count(
                label=constants.MON_APP_LABEL, expected_count=expected_mon_count
            )

        time.sleep(600)

        # scale the deployments back to 1
        for mon_dep in mon_deps:
            logger.info(f"Recovering mon by scaling up the mon deployment {mon_dep}")
            modify_deployment_replica_count(mon_dep, 1)
        wait_for_pods_by_label_count(
            label=constants.MON_APP_LABEL, expected_count=5, timeout=300
        )

    @polarion_id("OCS-5061")
    def test_single_osd_failure(self):
        """
        Test single osd failure with cephFS/RBD workloads running in the background

        """
        logger.info("testing single osd failure scenarios")
        sc_obj = StretchCluster()

        # get osd-pod of a zone where the cnv
        # workloads are running
        pod_objs = get_all_pods(namespace=CNV_WORKLOAD_NAMESPACE)
        assert len(pod_objs) != 0, "No vmi pod instances are running"
        node_obj = get_pod_node(pod_objs[0])
        osd_pods_in_zone = sc_obj.get_osd_pods_in_a_zone(
            node_obj.get()["metadata"]["labels"][constants.ZONE_LABEL]
        )
        osd_pod_to_fail = random.choice(osd_pods_in_zone).name

        # get the deployment of the osd-pod
        osd_dep = get_deployment_name(osd_pod_to_fail)

        # scale the deployment of osd to 0
        # and wait 10 mins
        logger.info(f"Failing osd by scaling down osd deployment {osd_dep}")
        if modify_deployment_replica_count(osd_dep, 0):
            time.sleep(600)

        # scale the deployment back to 1
        logger.info(f"Recovering osd by scaling up osd deployment {osd_dep}")
        modify_deployment_replica_count(osd_dep, 1)
