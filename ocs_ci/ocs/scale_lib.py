import logging
import threading
import random

from tests import helpers
from ocs_ci.framework import config
from ocs_ci.ocs.resources import pod, pvc, storage_cluster
from ocs_ci.ocs import constants, cluster, machine, node
from ocs_ci.ocs.exceptions import (
    UnavailableResourceException, UnexpectedBehaviour, CephHealthException
)

logger = logging.getLogger(__name__)


class FioPodScale(object):
    """

    """
    def __init__(
        self, kind='Pod', pod_dict_path=constants.NGINX_POD_YAML,
        node_selector=constants.SCALE_NODE_SELECTOR
    ):
        """
        Initializer function

        Args:
            kind (str): Kind of service POD or DeploymentConfig
            pod_dict_path (yaml): Pod yaml
            node_selector (dict): Pods will be created in this node_selector
                eg: {'nodetype': 'app-pod'}

        """
        self._kind = kind
        self._pod_dict_path = pod_dict_path
        self._node_selector = node_selector
        self._set_dc_deployment()
        self.namespace_list = list()

    @property
    def kind(self):
        return self._kind

    @property
    def pod_dict_path(self):
        return self._pod_dict_path

    @property
    def node_selector(self):
        return self._node_selector

    def _set_dc_deployment(self):
        """
        Set dc_deployment True or False based on Kind
        """
        if self.kind == 'DeploymentConfig':
            self.dc_deployment = True
        else:
            self.dc_deployment = False

    def create_and_set_namespace(self):
        """
        Create and set namespace for the pods to be created
        Create sa_name if Kind if DeploymentConfig
        """
        self.namespace_list.append(helpers.create_project())
        self.namespace = self.namespace_list[-1].namespace
        if self.dc_deployment:
            self.sa_name = helpers.create_serviceaccount(self.namespace)
            helpers.add_scc_policy(
                sa_name=self.sa_name.name, namespace=self.namespace
            )
        else:
            self.sa_name = None

    def create_multi_pvc_pod(
        self, pods_per_iter=5, io_runtime=3600, start_io=False
    ):
        """
        Function to create PVC of different type and attach them to PODs and start IO.
        Args:
            pods_per_iter (int): Number of PVC-POD to be created per PVC type
                eg: If 2 then 8 PVC+POD will be created with 2 each of 4 PVC types
            io_runtime (sec): Fio run time in seconds
            start_io (bool): If True start IO else don't

        Returns:
            pod_objs (obj): Objs of all the PODs created
            pvc_objs (obj): Objs of all the PVCs created
        """
        rbd_sc = helpers.default_storage_class(constants.CEPHBLOCKPOOL)
        cephfs_sc = helpers.default_storage_class(constants.CEPHFILESYSTEM)
        pvc_size = f"{random.randrange(5, 105, 5)}Gi"
        logging.info(f"Create {pods_per_iter} PVCs and PODs")
        cephfs_pvcs = helpers.create_multiple_pvc_parallel(
            sc_obj=cephfs_sc, namespace=self.namespace, number_of_pvc=pods_per_iter,
            size=pvc_size, access_modes=[constants.ACCESS_MODE_RWO, constants.ACCESS_MODE_RWX]
        )
        rbd_pvcs = helpers.create_multiple_pvc_parallel(
            sc_obj=rbd_sc, namespace=self.namespace, number_of_pvc=pods_per_iter,
            size=pvc_size, access_modes=[constants.ACCESS_MODE_RWO, constants.ACCESS_MODE_RWX]
        )
        # Appending all the pvc_obj and pod_obj to list
        pvc_objs, pod_objs = ([] for i in range(2))
        pvc_objs.extend(cephfs_pvcs + rbd_pvcs)

        # Create pods with above pvc list
        cephfs_pods = helpers.create_pods_parallel(
            cephfs_pvcs, self.namespace, constants.CEPHFS_INTERFACE,
            pod_dict_path=str(self.pod_dict_path), sa_name=self.sa_name.name,
            dc_deployment=self.dc_deployment, node_selector=self.node_selector
        )
        rbd_rwo_pvc, rbd_rwx_pvc = ([] for i in range(2))
        for pvc_obj in rbd_pvcs:
            if pvc_obj.get_pvc_access_mode == constants.ACCESS_MODE_RWX:
                rbd_rwx_pvc.append(pvc_obj)
            else:
                rbd_rwo_pvc.append(pvc_obj)
        rbd_rwo_pods = helpers.create_pods_parallel(
            rbd_rwo_pvc, self.namespace, constants.CEPHBLOCKPOOL,
            pod_dict_path=str(self.pod_dict_path), sa_name=self.sa_name.name,
            dc_deployment=self.dc_deployment, node_selector=self.node_selector
        )
        rbd_rwx_pods = helpers.create_pods_parallel(
            rbd_rwx_pvc, self.namespace, constants.CEPHBLOCKPOOL,
            pod_dict_path=str(self.pod_dict_path), sa_name=self.sa_name.name,
            dc_deployment=self.dc_deployment, raw_block_pv=True,
            node_selector=self.node_selector
        )
        temp_pod_objs = list()
        temp_pod_objs.extend(cephfs_pods + rbd_rwo_pods)

        # Appending all the pod_obj to list
        pod_objs.extend(temp_pod_objs + rbd_rwx_pods)

        fio_size = self.get_size_based_on_cls_usage()
        fio_rate = self.get_rate_based_on_cls_iops()

        # Start IO
        if start_io:
            threads = list()
            for pod_obj in temp_pod_objs:
                process = threading.Thread(
                    target=pod_obj.run_io, kwargs={
                        'storage_type': 'fs', 'size': fio_size,
                        'runtime': io_runtime, 'rate': fio_rate
                    }
                )
                process.start()
                threads.append(process)
            for pod_obj in rbd_rwx_pods:
                process = threading.Thread(
                    target=pod_obj.run_io, kwargs={
                        'storage_type': 'block', 'size': fio_size,
                        'runtime': io_runtime, 'rate': fio_rate
                    }
                )
                process.start()
                threads.append(process)
            for process in threads:
                process.join()

        return pod_objs, pvc_objs

    def get_size_based_on_cls_usage(self, custom_size_dict=None):
        """
        Function to check cls capacity suggest IO write to cluster
        Args:
            custom_size_dict (dict): Dictionary of size param to be used during IO run.
                eg: size_dict = {'usage_below_60': '2G', 'usage_60_70': '512M',
                'usage_70_80': '10M', 'usage_80_85': '512K', 'usage_above_85': '10K'}
            WARN: Make sure dict key is same as above example.
        Returns:
            size (str): IO size to be considered for cluster env
        """
        osd_dict = cluster.get_osd_utilization()
        if custom_size_dict:
            size_dict = custom_size_dict
        else:
            size_dict = {
                'usage_below_40': '1G', 'usage_40_60': '128M', 'usage_60_70': '10M',
                'usage_70_80': '5M', 'usage_80_85': '512K', 'usage_above_85': '10K'
            }
        temp = 0
        for k, v in osd_dict.items():
            if temp <= v:
                temp = v
        if temp <= 40:
            size = size_dict['usage_below_40']
        elif 40 < temp <= 50:
            size = size_dict['usage_40_50']
        elif 60 < temp <= 70:
            size = size_dict['usage_60_70']
        elif 70 < temp <= 80:
            size = size_dict['usage_70_80']
        elif 80 < temp <= 85:
            size = size_dict['usage_80_85']
        else:
            size = size_dict['usage_above_85']
            logging.warn(f"One of the OSD is near full {temp}% utilized")
        return size

    def get_rate_based_on_cls_iops(self, custom_iops_dict=None, osd_size=2048):
        """
        Function to check ceph cluster iops and suggest rate param for fio.
        Args:
            custom_iops_dict (dict): Dictionary of rate param to be used during IO run.
                eg: iops_dict = {'usage_below_40%': '16k', 'usage_40%_60%': '8k',
                'usage_60%_80%': '4k', 'usage_80%_95%': '2K'}
            WARN: Make sure dict key is same as above example.
            osd_size (int): Size of the OSD in GB
        Returns:
            rate_param (str): Rate parm for fio based on ceph cluster IOPs
        """
        # Check for IOPs limit percentage of cluster and accordingly suggest fio rate param
        cls_obj = cluster.CephCluster()
        iops = cls_obj.get_iops_percentage(osd_size=osd_size)
        if custom_iops_dict:
            iops_dict = custom_iops_dict
        else:
            iops_dict = {
                'usage_below_40%': '8k', 'usage_40%_60%': '8k',
                'usage_60%_80%': '4k', 'usage_80%_95%': '2K'
            }
        if (iops * 100) <= 40:
            rate_param = iops_dict['usage_below_40%']
        elif 40 < (iops * 100) <= 60:
            rate_param = iops_dict['usage_40%_60%']
        elif 60 < (iops * 100) <= 80:
            rate_param = iops_dict['usage_60%_80%']
        elif 80 < (iops * 100) <= 95:
            rate_param = iops_dict['usage_80%_95%']
        else:
            logging.warn(f"Cluster iops utilization is more than {iops * 100} percent")
            raise UnavailableResourceException("Overall Cluster utilization is more than 95%")
        return rate_param

    def create_scale_pods(
        self, scale_count=1500, pods_per_iter=2, instance_type='m5.4xlarge',
        io_runtime=None, start_io=None
    ):
        """
        scale_count (int): Scale pod+pvc count
        pods_per_iter (int): Number of PVC-POD to be created per PVC type
                eg: If 2 then 8 PVC+POD will be created with 2 each of 4 PVC types
        instance_type (str): Type of aws instance
        io_runtime (sec): Fio run time in seconds
        start_io (bool): If True start IO else don't

        """
        all_pod_obj = list()
        if config.ENV_DATA['deployment_type'] == 'ipi' and config.ENV_DATA['platform'].lower() == 'aws':
            # Create machineset for app worker nodes, which will create one app worker node
            self.ms_name = machine.create_custom_machineset(instance_type=instance_type, zone='a')
            machine.wait_for_new_node_to_be_ready(self.ms_name)
            self.app_worker_nodes = machine.get_machine_from_machineset(self.ms_name)

        # Create namespace
        self.create_and_set_namespace()

        # Continue to iterate till the scale pvc limit is reached
        while True:
            if scale_count <= len(all_pod_obj):
                logger.info(f"Scaled {scale_count} pvc and pods")

                # TODO: Check either cluster is in rebalance state.
                # # Check for pg_balancer
                # obj = CephCluster()
                # if not obj.get_rebalance_status():
                #     logger.warn(f"Cluster health is in WARN, rebalance in-progress")
                #     logger.info(f"Wait for rebalance to complete to validate pg_balancer")
                #     return False
                if cluster.validate_pg_balancer():
                    logging.info("OSD consumption and PG distribution is good to continue")
                else:
                    raise UnexpectedBehaviour("Unequal PG distribution to OSDs")

                break
            else:
                logger.info(f"Create {pods_per_iter} pods & pvc")
                pod_obj, pvc_obj = self.create_multi_pvc_pod(
                    pods_per_iter, io_runtime, start_io
                )
                all_pod_obj.extend(pod_obj)
                try:
                    # Check enough resources available in the dedicated app workers
                    if add_worker_based_on_cpu_utilization(
                        machineset_name=self.ms_name, node_count=1, expected_percent=65,
                        role_type='app,worker'
                    ):
                        logging.info(f"Nodes added for app pod creation")
                    else:
                        logging.info(f"Existing resource are enough to create more pods")

                    # Check for ceph cluster OSD utilization
                    if not cluster.validate_osd_utilization(osd_used=75):
                        logging.info("Cluster OSD utilization is below 75%")
                    elif cluster.validate_osd_utilization(osd_used=80):
                        logger.warn("Cluster OSD utilization is above 75%")
                    else:
                        raise CephHealthException("Cluster OSDs are near full")

                    # Check for 200 pods per namespace
                    pod_objs = pod.get_all_pods(namespace=self.namespace_list[-1].namespace)
                    if len(pod_objs) >= 200:
                        self.create_and_set_namespace()

                except UnexpectedBehaviour:
                    logging.error(
                        f"Scaling of cluster failed after {len(all_pod_obj)} pod creation"
                    )
                    raise UnexpectedBehaviour(
                        f"Scaling PVC+POD failed analyze setup and log for more details"
                    )

    def cleanup(self):
        """
        Function to tear down
        """
        # Delete all pods, pvcs and namespaces
        for namespace in self.namespace_list:
            helpers.delete_objs_parallel(pod.get_all_pods(namespace=namespace.name))
            helpers.delete_objs_parallel(pvc.get_all_pvc_objs(namespace=namespace.name))
            namespace.delete()
        # Delete machineset which will delete respective nodes too
        if self.ms_name:
            machine.delete_custom_machineset(self.ms_name)


def add_required_osd_count(total_osd_nos=3):
    """
    Function to add required OSD count and if count is less than expected
    Args:
        total_osd_nos (int): OSD count per node.
    Returns:
        cluster_total_osd_count (int): Total OSD count of overall cluster.
    """
    # Make sure setup has required number of OSDs
    actual_osd_count = cluster.count_cluster_osd()
    ocs_nodes = node.get_typed_nodes(node_type='worker')
    expected_osd_count = len(ocs_nodes) * total_osd_nos
    if actual_osd_count == expected_osd_count:
        logging.info(f"Setup has OSD count as per OCS workers")
    else:
        osd_size = storage_cluster.get_osd_size()
        calc = (expected_osd_count - actual_osd_count) / len(ocs_nodes)
        storage_cluster.add_capacity(osd_size * calc)
        logging.info(f"Now setup has expected osd count {expected_osd_count}")
    return cluster.count_cluster_osd()


def add_worker_based_on_cpu_utilization(
    machineset_name, node_count, expected_percent, role_type
):
    """
    Function to evaluate CPU utilization of nodes and add node if required.
    Args:
        machineset_name (str): Machineset_name to add more nodes if required.
        node_count (int): Additional nodes to be added
        expected_percent (int): Expected utilization precent
        role_type (str): To add type to the nodes getting added
    Returns:
        bool: True if Nodes gets added, else false.
    """
    # Check for CPU utilization on each nodes
    if config.ENV_DATA['deployment_type'] == 'ipi' and config.ENV_DATA['platform'].lower() == 'aws':
        app_nodes = node.get_typed_nodes(node_type=role_type)
        uti_dict = node.get_node_resource_utilization_from_oc_describe(node_type=role_type)
        uti_high_nodes, uti_less_nodes = ([] for i in range(2))
        for node_obj in app_nodes:
            utilization_percent = uti_dict[f"{node_obj.name}"]['cpu']
            if utilization_percent > expected_percent:
                uti_high_nodes.append(node_obj.name)
            else:
                uti_less_nodes.append(node_obj.name)
        if len(uti_less_nodes) < 1:
            count = machine.get_replica_count(machine_set=machineset_name)
            machine.add_node(machine_set=machineset_name, count=(count + node_count))
            machine.wait_for_new_node_to_be_ready(machineset_name)
            return True
        else:
            logging.info("Enough resource available for more pod creation")
            return False
    elif config.ENV_DATA['deployment_type'] == 'upi' and config.ENV_DATA['platform'].lower() == 'vshpere':
        pass
