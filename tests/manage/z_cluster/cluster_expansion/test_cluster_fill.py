import logging
import pytest
from concurrent.futures import ThreadPoolExecutor

from ocs_ci.framework.testlib import tier4, ignore_leftovers, ManageTest
from tests.manage.z_cluster.cluster_expansion.create_delete_pvc_parallel import test_create_delete_pvcs
from tests.manage.mcg.helpers import s3_io
from tests.manage.z_cluster import tier4_helpers

from ocs_ci.framework.pytest_customization.marks import (filter_insecure_request_warning,
)

logger = logging.getLogger(__name__)


@ignore_leftovers
@filter_insecure_request_warning
@tier4
class TestAddNode(ManageTest):
    def test_run_io_multiple_dc_pods(self,project_factory,multi_dc_pod,multi_pvc_factory, pod_factory,mcg_obj, awscli_pod, bucket_factory):
        cluster_pods_to_create, cluster_fill_iterations = tier4_helpers.cluster_fill_get_pod_iter("75")
        num_of_pvcs = int(cluster_pods_to_create/3)

        project = project_factory()

        rwo_rbd_pods = multi_dc_pod(num_of_pvcs=num_of_pvcs, pvc_size=100,
                                    project=project, access_mode="RWO", pool_type='rbd')


        rwo_cephfs_pods = multi_dc_pod(num_of_pvcs=num_of_pvcs, pvc_size=100,
                                       project=project, access_mode="RWO", pool_type='cephfs')
        rwx_cephfs_pods = multi_dc_pod(num_of_pvcs=num_of_pvcs, pvc_size=100,  #
                                       project=project, access_mode="RWX", pool_type='cephfs')

        pods = rwo_rbd_pods + rwo_cephfs_pods + rwx_cephfs_pods

        executor = ThreadPoolExecutor(max_workers=32)

        for pod in pods:
            executor.submit(tier4_helpers.cluster_fillup_fun,pod,cluster_fill_iterations)

        executor.submit(test_create_delete_pvcs,multi_pvc_factory, pod_factory, project)
        executor.submit(s3_io, mcg_obj, awscli_pod, bucket_factory)
        assert tier4_helpers.check_cluster_size(50)
        logging.info('Pass')
        # #Todo - integrate add node and add capacity UI automation
