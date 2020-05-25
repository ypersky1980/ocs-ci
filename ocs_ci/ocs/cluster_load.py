"""
A module for cluster load related functionalities

"""
import logging
import time

from ocs_ci.utility import templating
from ocs_ci.ocs import constants
from ocs_ci.ocs.cluster import (
    CephCluster, get_osd_pods_memory_sum, get_percent_used_capacity
)
from ocs_ci.ocs.resources.pod import delete_deploymentconfig


logger = logging.getLogger(__name__)


class ClusterLoad:
    """
    A class for cluster load functionalities

    """

    def __init__(self, pvc_factory, sa_factory, pod_factory, target_percentage):
        """
        Initializer for ClusterLoad

        Args:
            pvc_factory (function): A call to pvc_factory function
            sa_factory (function): A call to service_account_factory function
            pod_factory (function): A call to pod_factory function
            target_percentage (float): The percentage of cluster load that is required.
                The value should be greater than 0 and smaller than 1

        """
        self.cl_obj = CephCluster()
        self.pvc_factory = pvc_factory
        self.sa_factory = sa_factory
        self.pod_factory = pod_factory
        self.target_percentage = target_percentage
        self.pod_objs = list()
        self.pvc_objs = list()
        self.pvc_size = int(get_osd_pods_memory_sum() * 0.5)
        self.io_file_size = f"{self.pvc_size - 1}G"

    def create_fio_pod(self):
        """
        Create a PVC, a service account and a DeploymentConfig of FIO pod

        """
        pvc_obj = self.pvc_factory(
            interface=constants.CEPHBLOCKPOOL, size=self.pvc_size,
            volume_mode=constants.VOLUME_MODE_BLOCK
        )
        service_account = self.sa_factory(pvc_obj.project)

        self.pvc_objs.append(pvc_obj)
        # Set new arguments with the updated file size to be used for
        # DeploymentConfig of FIO pod creation
        fio_dc_data = templating.load_yaml(constants.FIO_DC_YAML)
        current_args = fio_dc_data['spec']['template']['spec']['containers'][0]['args']
        new_args = [arg for arg in current_args if "--filesize=" not in arg]
        new_args.append(f"--filesize={self.io_file_size}")
        pod_obj = self.pod_factory(
            pvc=pvc_obj, pod_dict_path=constants.FIO_DC_YAML,
            raw_block_pv=True, deployment_config=True,
            service_account=service_account, command_args=new_args
        )
        self.pod_objs.append(pod_obj)
        logger.info(f"Waiting 20 seconds for the IOs to kick-in on pod {pod_obj.name}")
        time.sleep(20)

    def delete_pod_and_pvc(self):
        """
        Delete DeploymentConfig with its pods and the PVC. Then, wait for the
        IO to be stopped

        """
        pod_name = self.pod_objs[-1].name
        delete_deploymentconfig(self.pod_objs[-1])
        self.pod_objs.remove(self.pod_objs[-1])
        self.pvc_objs[-1].delete()
        self.pvc_objs[-1].ocp.wait_for_delete(self.pvc_objs[-1].name)
        self.pvc_objs.remove(self.pvc_objs[-1])
        logger.info(f"Waiting for IO to be stopped on pod {pod_name}")
        time.sleep(20)

    def reach_cluster_load_percentage_in_throughput(self):
        """
        Reach the cluster throughput limit and then drop to the given target percentage.
        The number of pods needed for the desired target percentage is determined by
        creating pods one by one, while examining if the cluster throughput is increased
        by more than 10%. When it doesn't increased by more than 10% anymore after
        the new pod started running IO, it means that the cluster throughput limit is
        reached. Then, the function deletes the pods that are not needed as they
        are the difference between the limit (100%) and the target percentage.
        This leaves the number of pods needed running IO for cluster throughput to
        be around the desired percentage.

        """
        if not 0.1 < self.target_percentage < 0.95:
            logger.warning(
                f"The target percentage is {self.target_percentage * 100}% which is not "
                f"within the accepted range. Therefore, IO will not be started"
            )
            return

        limit_reached = False
        cluster_limit = None
        time_to_wait = 60 * 30
        time_before = time.time()
        low_diff_counter = 0

        current_throughput = self.cl_obj.calc_average_throughput()

        while not limit_reached:
            logger.info(
                f"The cluster average collected throughput BEFORE starting "
                f"IOs on the newly created pod is {current_throughput} Mb/s"
            )

            self.create_fio_pod()

            previous_throughput = current_throughput
            current_throughput = self.cl_obj.calc_average_throughput()
            logger.info(
                f"\n===========================================================\n"
                f"The cluster average collected throughput AFTER starting IO\n"
                f"on the newly created pod is {current_throughput} Mb/s, while "
                f"before, it\nwas {previous_throughput} Mb/s. The number of "
                f"pods running IOs is {len(self.pod_objs) - 1}"
                f"\n==========================================================="
            )

            if current_throughput > 20:
                tp_diff = (current_throughput / previous_throughput * 100) - 100
                logger.info(
                    f"\n========================================\n"
                    f"The throughput difference after starting\nIOs"
                    f" on the newly created pod is {tp_diff:.2f}%"
                    f"\n========================================"
                )
                if -10 < tp_diff < 10:
                    low_diff_counter += 1
                else:
                    low_diff_counter = 0
                if low_diff_counter > 1:
                    limit_reached = True
                    cluster_limit = current_throughput
                    logger.info(
                        f"\n===========================================\n"
                        f"The cluster throughput limit is {cluster_limit} Mb/s"
                        f"\n==========================================="
                    )

            if time.time() > time_before + time_to_wait:
                logger.warning(
                    f"\n===============================================\n"
                    f"Could not determine the cluster throughput limit\n"
                    f"within the given {time_to_wait} timeout. Breaking"
                    f"\n==============================================="
                )
                limit_reached = True
                cluster_limit = current_throughput

            if current_throughput < 20:
                if time.time() > time_before + (time_to_wait * 0.5):
                    logger.warning(
                        f"\n========================================\n"
                        f"Waited for {time_to_wait * 0.5} seconds and the"
                        f"\nthroughput is less than 20 Mb/s. Breaking"
                        f"\n========================================"
                    )
                    cluster_limit = current_throughput
                if len(self.pod_objs) > 8:
                    logger.warning(
                        f"\n=================================\n"
                        f"The number of pods running IO is {len(self.pod_objs)} "
                        f"and the\nthroughput is less than 20 Mb/s. Breaking"
                        f"\n================================="
                    )
                    limit_reached = True
                    cluster_limit = current_throughput

            cluster_used_space = get_percent_used_capacity()
            if cluster_used_space > 50:
                logger.warning(
                    f"\n===============================================\n"
                    f"Cluster used space is {cluster_used_space}%. Could "
                    f"not reach\nthe cluster throughput limit before the\n"
                    f"used spaced reached 50%. Breaking"
                    f"\n==============================================="
                )
                limit_reached = True
                cluster_limit = current_throughput

        target_throughput = cluster_limit * self.target_percentage
        logger.info(f"The target throughput is {target_throughput} Mb/s")
        logger.info(f"The current throughput is {current_throughput} Mb/s")
        logger.info(
            "Start deleting pods that are running IO one by one while comparing "
            "the current throughput utilization with the target one. The goal is "
            "to reach cluster throughput utilization that is more or less the "
            "target throughput percentage"
        )
        while current_throughput > (target_throughput * 1.05) and len(self.pod_objs) > 1:
            self.delete_pod_and_pvc()
            current_throughput = self.cl_obj.calc_average_throughput()
            logger.info(
                f"The cluster average collected throughput after deleting "
                f"a pod is {current_throughput} Mb/s. The number "
                f"of pods running IOs is {len(self.pod_objs)}"
            )

        logger.info(
            f"\n==============================================\n"
            f"The number of pods that will continue running"
            f"\nIOs is {len(self.pod_objs)} at a load of {current_throughput} Mb/s"
            f"\n=============================================="
        )
