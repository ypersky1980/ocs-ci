import logging
from concurrent.futures.thread import ThreadPoolExecutor
logger = logging.getLogger(__name__)



def io_cluster_fillup(pods):
    logging.info('IO started')
    rbd_rawblock_pods = pods[5:10]
    non_rawblock_pods = (list(set(pods) - set(rbd_rawblock_pods)))
    storage_type = 'block'
    with ThreadPoolExecutor() as p:
        for dc_pod in non_rawblock_pods:
            p.submit(fill_up, dc_pod)
        for rawblockpod in rbd_rawblock_pods:
            p.submit(rawblockpod.run_io, storage_type=storage_type,size='100G')

    return True

def fill_up(pod):
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
