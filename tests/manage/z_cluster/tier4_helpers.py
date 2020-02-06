import logging
import time
from ocs_ci.ocs.cluster import CephCluster
import ocs_ci.ocs.resources.pod as pod


logger = logging.getLogger(__name__)

def raw_block_io(pod,size='100G'):
    pod.run_io(storage_type = 'block', size = size)
    return True


def cluster_fillup(pod):
    """
    Todo
    Make IO sizes configurable
    """
    logger.info('filling up..')
    cmd1 = """
mkdir /mnt/temp{1..30} ;curl http://download.ceph.com/tarballs/ceph_14.2.4.orig.tar.gz --output /mnt/temp1/ceph.tar.gz;
    """
    cmd2 = """
    for i in {1..30}; do cp /mnt/temp1/ceph.tar.gz /mnt/temp1/ceph_$i & done
"""
    cmd3 = """
for i in {2..30};
do cp -r /mnt/temp1/* /mnt/temp$i/. &
done
    """
    pod.exec_bash_cmd_on_pod(command=cmd1)
    pod.exec_bash_cmd_on_pod(command=cmd2)
    pod.exec_bash_cmd_on_pod(command=cmd3)
    logger.info('started..')


def check_cluster_size(size):
    ceph_obj = CephCluster()
    ct_pod = pod.get_ceph_tools_pod()
    retries = 0
    while True:
        if ceph_obj.get_used_space(ct_pod) >= size:
            logger.info('used space has reached...')
            return True
        else:
            logger.info('rechecking.......')
            retries += 1
            pass
        if retries > 20 and ceph_obj.get_used_space(ct_pod) <= 5:
            logger.error('IOs not happening ')
            return False
        time.sleep(1200)
