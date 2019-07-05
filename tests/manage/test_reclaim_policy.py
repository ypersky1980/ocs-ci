import logging
import pytest
from ocs_ci.framework.testlib import tier1, ManageTest
from tests import helpers
from ocs_ci.ocs import ocp
from ocs_ci.ocs.resources.pod import list_ceph_images

log = logging.getLogger(__name__)
PVC = ocp.OCP(kind='PersistentVolumeClaim', namespace='openshift-storage')


@pytest.fixture(scope='class')
def test_fixture(request):
    """
    This is a test fixture
    """
    self = request.node.cls

    def finalizer():
        teardown(self)

    request.addfinalizer(finalizer)
    setup(self)


def setup(self):
    """
    Setting up a secret and storage class
    """

    self.secret_obj = helpers.create_secret('CephBlockPool')
    self.sc_obj_retain = helpers.create_storage_class(
        interface_type='CephBlockPool', interface_name='rbd',
        secret_name=self.secret_obj.name, reclaim_policy='Retain'
    )
    self.sc_obj_delete = helpers.create_storage_class(
        interface_type='CephBlockPool', interface_name='rbd',
        secret_name=self.secret_obj.name, reclaim_policy='Delete'
    )


def teardown(self):
    """
    Deleting secret
    """
    assert self.sc_obj_retain.delete()
    assert self.sc_obj_delete.delete()
    assert self.secret_obj.delete()


@tier1
@pytest.mark.usefixtures(test_fixture.__name__)
class TestReclaimPolicy(ManageTest):
    """
    Automates the following test cases
     OCS-383 - OCP_Validate Retain policy is honored
     OCS-384 - OCP_Validate Delete policy is honored
    """

    def test_reclaim_policy_retain(self):
        """
        Calling functions for pvc invalid name and size
        """
        log.info("sc_retain")
        sc_name = self.sc_obj_retain.name
        pvc_count = len(list_ceph_images())
        pvc_obj = helpers.create_pvc(sc_name, pvc_name="pvc-retain")
        pv_name = pvc_obj.get()['spec']['volumeName']
        pv_namespace = pvc_obj.get()['metadata']['namespace']
        pv_obj = ocp.OCP(kind='PersistentVolume', namespace=pv_namespace)
        assert pvc_obj.delete()
        pvc_obj.ocp.wait_for_delete(resource_name=pvc_obj.name)
        assert pv_obj.get(pv_name).get('status').get('phase') == 'Released', (
            f"Status of PV {pv_obj.get(pv_name)} is not 'Released'"
        )
        log.info("Status of PV is Released")
        assert pvc_count + 1 == len(list_ceph_images())
        assert pv_obj.delete(resource_name=pv_name)
        # TODO: deletion of ceph rbd image, blocked by BZ#1723656

    def test_reclaim_policy_delete(self):
        """
        Test to validate storage class with reclaim policy "Delete"
        """
        sc_name = self.sc_obj_delete.name
        pvc_obj = helpers.create_pvc(sc_name, pvc_name="pvc-delete")
        pv_name = pvc_obj.get()['spec']['volumeName']
        pv_namespace = pvc_obj.get()['metadata']['namespace']
        pv_obj = ocp.OCP(kind='PersistentVolume', namespace=pv_namespace)
        assert pvc_obj.delete()
        pvc_obj.ocp.wait_for_delete(resource_name=pvc_obj.name)
        assert pv_name not in pv_obj.get()['items']
        # TODO: deletion of ceph rbd image, blocked by BZ#1723656
