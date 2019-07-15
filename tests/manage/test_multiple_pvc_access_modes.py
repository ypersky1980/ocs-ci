import time

from ocs_ci.framework import config
from ocs_ci.ocs.exceptions import UnexpectedBehaviour
from ocs_ci.ocs.ocp import OCP

import logging
import pytest
import random

from ocs_ci.ocs import constants, exceptions
from ocs_ci.ocs import constants
from ocs_ci.framework.testlib import tier1, ManageTest
from ocs_ci.ocs.parallel import parallel
from ocs_ci.ocs.resources.pod import Pod
from ocs_ci.utility.retry import retry
from tests import helpers
from ocs_ci.framework.testlib import ManageTest
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.utility import templating
from ocs_ci.ocs import constants, ocp, exceptions


from tests.fixtures import (
    create_rbd_storageclass,create_ceph_block_pool,
    create_rbd_secret,
    create_cephfs_secret, create_cephfs_storageclass)



log = logging.getLogger(__name__)

@tier1
@pytest.mark.usefixtures(
    create_cephfs_secret.__name__,
    create_cephfs_storageclass.__name__,
)
class TestMultiplePvcAccessModesIOs(ManageTest):
    def test_multiple_pvc_access_modes_ios_cephfs(self):
        access_modes = ['ReadWriteOnce', 'ReadWriteMany']
        interface = constants.CEPHFILESYSTEM
        self.pvc_objs = []
        number_of_pvc = 5
        number_of_pod = 5
        self.pod_objs = []
        if interface == constants.CEPHBLOCKPOOL:
            access_mode = 'ReadWriteOnce'
            self.pvc_objs = helpers.create_multiple_pvc(sc_name=self.sc_obj.name, number_of_pvc=number_of_pvc,
                                                        access_mode=access_mode)
            for pvc in self.pvc_objs:
                wait = True
                for n in range(number_of_pod):
                    pod = helpers.create_pod(constants.CEPHBLOCKPOOL, pvc.name, wait=wait)
                    self.pod_objs.append(pod)
                    logging.info(pod.get().get('status').get('phase'))
                    if pod.get().get('status').get('phase') == 'Running':
                        wait = False

        else:
            for access_mode in access_modes:
                pvc_objs = helpers.create_multiple_pvc(sc_name=self.sc_obj.name, number_of_pvc=number_of_pvc,
                                                            access_mode=access_mode)
                if access_mode == 'ReadWriteOnce':
                    self.rwo_pvcs = pvc_objs

                else:
                    self.rwx_pvcs = pvc_objs
            for pvc in self.rwo_pvcs:
                wait = True
                for n in range(number_of_pod):
                    pod = helpers.create_pod(constants.CEPHFILESYSTEM, pvc.name, wait=wait)
                    self.pod_objs.append(pod)
                    logging.info(pod.get().get('status').get('phase'))
                    if pod.get().get('status').get('phase') == 'Running':
                        wait = False

            for pvc in self.rwx_pvcs:
                for n in range(number_of_pod):
                    pod = helpers.create_pod(constants.CEPHFILESYSTEM, pvc.name)
                    self.pod_objs.append(pod)
                    logging.info(pod.get().get('status').get('phase'))




        self.bound_pods = []
        self.unbound_pods = []
        for pod in self.pod_objs:
            if pod.get().get('status').get('phase') == 'Running':
                self.bound_pods.append(pod)
            else:
                self.unbound_pods.append(pod)

        with parallel() as p:
            for pod in self.bound_pods:
                logging.info(f'running io on pod {pod.name}')
                p.spawn(pod.run_io, 'fs', f'{random.randint(1,5)}G')
        logging.info(f'num of pods {len(self.pod_objs)}')

        logging.info(f'num of bound pods {len(self.bound_pods)}')
        logging.info(f'num of unbound pods {len(self.unbound_pods)}')

        for pod in self.bound_pods:
            pod.delete()

        self.new_bounded_pods = []
        self.leftover_unbound_pods = []
        time.sleep(180)
        for pod in self.unbound_pods:
            logging.info(pod.get().get('status').get('phase'))
            if pod.get().get('status').get('phase') == 'Running':
                self.new_bounded_pods.append(pod)
            else:
                self.leftover_unbound_pods.append(pod)

        logging.info(f'num of newly bound pods {len(self.new_bounded_pods)}')
        logging.info(f'num of newly unbound pods {len(self.leftover_unbound_pods)}')

        with parallel() as p:
            for pod in self.new_bounded_pods:
                p.spawn(pod.run_io, 'fs', f'{random.randint(1,5)}G')

        pods = self.new_bounded_pods+self.leftover_unbound_pods
        delete_pods(pods)

        delete_pvcs(self.pvc_objs)


def delete_pods(pod_objs):
    for pod in pod_objs:
        pod.delete()

    return True


def delete_pvcs(pvc_objs):
    for pvc in pvc_objs:
        pvc.delete()

    return True
