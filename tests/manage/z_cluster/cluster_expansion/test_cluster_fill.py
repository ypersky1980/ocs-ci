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
        project = project_factory()
        pods = multi_dc_pod(num_of_pvcs=10,pvc_size=200,project=project)
        rbd_rawblock_pods =  pods[int(len(pods)/2)::]
        non_rawblock_pods = (list(set(pods) - set(rbd_rawblock_pods)))

        executor = ThreadPoolExecutor(max_workers=10)
        for pod in non_rawblock_pods:
            executor.submit(tier4_helpers.cluster_fillup, pod)
        for rawblockpod in rbd_rawblock_pods:
            executor.submit(tier4_helpers.raw_block_io,rawblockpod)

        executor.submit(test_create_delete_pvcs,multi_pvc_factory, pod_factory, project)
        executor.submit(s3_io, mcg_obj, awscli_pod, bucket_factory)
        assert tier4_helpers.check_cluster_size(50)
        logging.info('Pass')
        #Todo - integrate add node and add capacity UI automation
