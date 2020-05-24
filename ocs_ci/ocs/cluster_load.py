"""
A module for cluster load related functionalities

"""
import logging
import time

import ocs_ci.ocs.constants as constant
from ocs_ci.ocs import constants
from ocs_ci.ocs.cluster import (
    CephCluster, get_osd_pods_memory_sum, get_percent_used_capacity
)
from ocs_ci.ocs.resources.pod import get_ceph_tools_pod


class ClusterLoad:
    """
    A class for cluster load functionalities

    """

    def __init__(self, pvc_factory, pod_factory, target_percentage):
        """
        Initializer for ClusterLoad

        Args:
            pvc_factory (function): A call to pvc_factory function
            pod_factory (function): A call to pod_factory function
            target_percentage (float): The percentage of cluster load that is required.
                The value should be greater than 0 and smaller than 1

        """
        self.logger = logging.getLogger(__name__)
        self.logger.propagate = True
        self.cl_obj = CephCluster()
        self.pvc_factory = pvc_factory
        self.pod_factory = pod_factory
        self.target_percentage = target_percentage
        self.target_throughput = None

    def reach_cluster_load_percentage_in_throughput(self, ):
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
            self.logger.warning(
                f"The target percentage is {self.target_percentage * 100}% which is not "
                f"within the accepted range. Therefore, IO will not be started"
            )
            return

        pvc_objs = list()
        pod_objs = list()
        limit_reached = False

        # IO params:
        storage_type = 'block'
        io_run_time = 100**3
        rate = '200M'
        bs = '128K'
        rw_ratio = 25
        pvc_size = int(get_osd_pods_memory_sum() * 0.5)
        io_file_size = f"{pvc_size - 1}G"

        def create_pvc_and_pod():
            pvc_obj = self.pvc_factory(
                interface=constant.CEPHBLOCKPOOL, size=pvc_size,
                volume_mode=constants.VOLUME_MODE_BLOCK
            )
            pvc_objs.append(pvc_obj)
            pod_obj = self.pod_factory(
                pvc=pvc_obj, pod_dict_path=constants.CSI_RBD_RAW_BLOCK_POD_YAML,
                raw_block_pv=True
            )
            pod_objs.append(pod_obj)
            return pod_obj

        pod_obj = create_pvc_and_pod()
        pod_obj.run_io(
            storage_type=storage_type, size=io_file_size,
            runtime=io_run_time, rate=rate, bs=bs, rw_ratio=rw_ratio
        )

        time.sleep(10)
        pod_obj = create_pvc_and_pod()

        current_throughput = self.cl_obj.calc_average_throughput()

        target_tp_reached_msg = None
        if self.target_throughput:
            target_tp_reached_msg = (
                f"\n=============================================\n"
                f"Current throughput, {current_throughput} Mb/s, surpassed"
                f"\nthe target throughput ({self.target_throughput} Mb/s)"
                f"\n============================================="
            )
            if current_throughput > self.target_throughput * 1.1:
                self.logger.info(target_tp_reached_msg)
                limit_reached = True

        time_to_wait = 60 * 30
        time_before = time.time()
        low_diff_counter = 0

        if not limit_reached:
            self.logger.info(
                f"\n==========================================================\n"
                f"Determining the cluster throughput limit. Once determined,"
                f"\nIOs will be reduced to load at {self.target_percentage * 100}% "
                f"of the cluster limit"
                f"\n=========================================================="
            )

        while not limit_reached:
            self.logger.info(
                f"The cluster average collected throughput BEFORE starting "
                f"IOs on the newly created pod is {current_throughput} Mb/s"
            )
            pod_obj.run_io(
                storage_type=storage_type, size=io_file_size,
                runtime=io_run_time, rate=rate, bs=bs, rw_ratio=rw_ratio
            )
            time.sleep(10)
            self.logger.info(
                f"While IO kicks-in on the previously created pod ({pod_obj.name}), "
                f"creating a new pod and PVC for the next iteration"
            )

            pod_obj = create_pvc_and_pod()

            previous_throughput = current_throughput
            current_throughput = self.cl_obj.calc_average_throughput()
            self.logger.info(
                f"\n===========================================================\n"
                f"The cluster average collected throughput AFTER starting IO\n"
                f"on the newly created pod is {current_throughput} Mb/s, while "
                f"before, it\nwas {previous_throughput} Mb/s. The number of "
                f"pods running IOs is {len(pod_objs) - 1}"
                f"\n==========================================================="
            )

            if not self.target_throughput:
                if current_throughput > 20:
                    tp_diff = (current_throughput / previous_throughput * 100) - 100
                    self.logger.info(
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
                        self.logger.info(
                            f"\n===========================================\n"
                            f"The cluster throughput limit is {cluster_limit} Mb/s"
                            f"\n==========================================="
                        )
            else:
                if current_throughput > self.target_throughput:
                    self.logger.info(target_tp_reached_msg)
                    limit_reached = True
            if time.time() > time_before + time_to_wait:
                msg = "determine the cluster throughput limit"
                if self.target_throughput:
                    msg = "reach the cluster throughput percentage"
                self.logger.warning(
                    f"\n===============================================\n"
                    f"Could not {msg}\n"
                    f"within the given {time_to_wait} timeout. Breaking"
                    f"\n==============================================="
                )
                limit_reached = True
                cluster_limit = current_throughput

            if current_throughput < 20:
                if time.time() > time_before + (time_to_wait * 0.5):
                    self.logger.warning(
                        f"\n========================================\n"
                        f"Waited for {time_to_wait * 0.5} seconds and the"
                        f"\nthroughput is less than 20 Mb/s. Breaking"
                        f"\n========================================"
                    )
                    cluster_limit = current_throughput
                if len(pod_objs) > 8:
                    self.logger.warning(
                        f"\n=================================\n"
                        f"The number of pods running IO is {len(pod_objs)} "
                        f"and the\nthroughput is less than 20 Mb/s. Breaking"
                        f"\n================================="
                    )
                    limit_reached = True
                    cluster_limit = current_throughput

            cluster_used_space = get_percent_used_capacity()
            if cluster_used_space > 50:
                msg = "limit"
                if self.target_throughput:
                    msg = "target percentage"
                self.logger.warning(
                    f"\n===============================================\n"
                    f"Cluster used space is {cluster_used_space}%. Could "
                    f"not reach\nthe cluster throughput {msg} before the\n"
                    f"used spaced reached 50%. Breaking"
                    f"\n==============================================="
                )
                limit_reached = True
                cluster_limit = current_throughput

        target_throughput = cluster_limit * self.target_percentage
        self.logger.info(f"The target throughput is {target_throughput} Mb/s")
        self.logger.info(f"The current throughput is {current_throughput} Mb/s")
        self.logger.info(
            "Start deleting pods that are running IO one by one while comparing "
            "the current throughput utilization with the target one. The goal is "
            "to reach cluster throughput utilization that is more or less the "
            "target throughput percentage"
        )
        while current_throughput > (target_throughput * 1.1) and len(pod_objs) > 1:
            pod_name = pod_objs[-1].name
            pod_objs[-1].delete()
            pod_objs[-1].ocp.wait_for_delete(pod_objs[-1].name)
            pod_objs.remove(pod_objs[-1])
            pvc_objs[-1].delete()
            pvc_objs[-1].ocp.wait_for_delete(pvc_objs[-1].name)
            pvc_objs.remove(pvc_objs[-1])
            self.logger.info(f"Waiting for IO to be stopped on pod {pod_name}")
            time.sleep(10)
            current_throughput = self.cl_obj.calc_average_throughput()
            self.logger.info(
                f"The cluster average collected throughput after deleting "
                f"pod {pod_name} is {current_throughput} Mb/s. The number "
                f"of pods running IOs is {len(pod_objs)}"
            )

        self.logger.info(
            f"\n==============================================\n"
            f"The number of pods that will continue running"
            f"\nIOs is {len(pod_objs)} at a load of {current_throughput} Mb/s"
            f"\n=============================================="
        )
        self.target_throughput = current_throughput

    def ensure_cluster_load(self):
        """
        Ensure cluster is loaded by checking the clsuter throuput and comparing
        it to the target load set by `reach_cluster_load_percentage_in_throughput`

        """
        self.cl_obj.toolbox = get_ceph_tools_pod()
        if self.cl_obj.is_health_ok():
            average_tp = self.cl_obj.calc_average_throughput(samples=16)
            if average_tp < self.target_throughput * 0.6:
                self.logger.warning(
                    "\n=================================================="
                    "==================================================\n"
                    "Cluster throughput dropped below 60% of the target "
                    "throughput for background IO load. Re-loading IOs"
                    "\n=================================================="
                    "=================================================="
                )
                self.reach_cluster_load_percentage_in_throughput()
