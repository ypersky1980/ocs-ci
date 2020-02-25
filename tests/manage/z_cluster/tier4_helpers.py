import logging
import time
from ocs_ci.ocs.cluster import CephCluster
import ocs_ci.ocs.resources.pod as pod
from ocs_ci.ocs.resources import storage_cluster


logger = logging.getLogger(__name__)

def raw_block_io(pod,io_config):
    pod.run_io(storage_type = 'block', size = io_config.get('file_size'))
    return True

#
# def cluster_fillup(pod, iterations=1):
#     logging.info("############################################")
#     logging.info(f"Filling up cluster FROM Pod: {pod.name}")
#     logging.info("############################################")
#
#     cmd1 = '''
#     mkdir /mnt/temp{1..30} ;
#     curl http://download.ceph.com/tarballs/ceph_14.2.4.orig.tar.gz --output /mnt/temp1/ceph.tar.gz;
#     '''
#     cmd2 = '''
#     for i in {1..10}; do cp /mnt/temp1/ceph.tar.gz /mnt/temp1/ceph_$i & done
#     '''
#     cmd3 = '''
#     for i in {2..30};
#     do cp -r /mnt/temp1/* /mnt/temp$i/. &
#     done
#     '''
#     ceph_obj = CephCluster()
#     ct_pod = pod.get_ceph_tools_pod()
#
#     pod.exec_sh_cmd_on_pod(command=cmd1)
#     pod.exec_sh_cmd_on_pod(command=cmd2)
#
#     for i in range(int(iterations)):
#         pod.exec_sh_cmd_on_pod(command=cmd3)
#         logging.info(f"***** Used space = {ceph_obj.get_used_space(ct_pod)}")
#
#     logger.info('started..')
#
#


def cluster_fillup(pod, iterations=1):
    logger.info(f'filling up on pod {pod.name}')

    cmd1 = '''
mkdir /mnt/temp{1..30} ;
curl http://download.ceph.com/tarballs/ceph_14.2.4.orig.tar.gz --output /mnt/temp1/ceph.tar.gz;
'''
    cmd2 = '''
for i in {1..10}; do cp /mnt/temp1/ceph.tar.gz /mnt/temp1/ceph_$i & done
'''
    cmd3 = '''
for i in {2..30};
do cp -r /mnt/temp1/* /mnt/temp$i/. &
done
'''
    pod.exec_bash_cmd_on_pod(command=cmd1)
    pod.exec_bash_cmd_on_pod(command=cmd2)
    for _ in range(iterations):
        pod.exec_sh_cmd_on_pod(command=cmd3)
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
        if retries > 5 and ceph_obj.get_used_space(ct_pod) <= 5:
            logger.error('IOs not happening ')
            return False
        time.sleep(1200)




#
# def gen_io_config(osd_size):
#     di = {}
#     if osd_size == 340:
#         di.update({'pvc_num': 10,'pvc_size': 50, 'dir_num': 15, 'file_size' : '50G'})
#
#     elif osd_size == 2024:
#         di.update({'pvc_num': 10,'pvc_size': 200, 'dir_num': 30, 'file_size' : '100G'})
#
#     else:
#         di.update({'pvc_num': 10,'pvc_size': 400, 'dir_num': 60, 'file_size' : '200G'})
#
#     return di


def cluster_fill_get_pod_iter(percent_capacity_to_fill=37):
    # this dictionary is based on the:
    # https://docs.google.com/spreadsheets/d/1XXqDNB6ujiQg--qyoJvB18GPgv_sIi8Xt4-c2tYtD1o/edit#gid=0&range=B15

    dict_pod_iter = {
        "25":
            {
                "512": {"pod": 3, "iteration": 1, 'file_size': 50},
                "2": {"pod": 6, "iteration": 2, 'file_size': 100},
                "4": {"pod": 24, "iteration": 1, 'file_size': 200}
            },
        "50":
            {
                "512": {"pod": 6, "iteration": 1,'file_size': 50},
                "2": {"pod": 12, "iteration": 2, 'file_size': 100},
                "4": {"pod": 24, "iteration": 2, 'file_size': 200}
            },
        "75":
            {
                "340": {"pod": 6, "iteration": 1,'file_size': 30},
                "512": {"pod": 9, "iteration": 1,'file_size': 50},
                "2": {"pod": 18, "iteration": 2,'file_size': 100},
                "4": {"pod": 36, "iteration": 2,'file_size': 200},
            },
        "85":
            {
                "340": {"pod": 6, "iteration": 1,'file_size': 30},
                "512": {"pod": 9, "iteration": 1,'file_size': 50},
                "2": {"pod": 21, "iteration": 2,'file_size': 100},
                "4": {"pod": 42, "iteration": 2,'file_size': 200},
            },
        "37":  # Testing purpose on ocs-ci based cluster. Not for production
            {
                "340": {"pod": 3, "iteration": 1}
            }

    }
    osd_size = storage_cluster.get_osd_size()
    pods = dict_pod_iter[percent_capacity_to_fill][osd_size]["pod"]
    iterations = dict_pod_iter[percent_capacity_to_fill][osd_size]["iteration"]
    logging.info(f"pods to create = {pods}. iterations to run = {iterations}")
    return pods, iterations


def cluster_fillup_fun(pod,iteration=1):
    logger.info(f'filling up on pod {pod.name}')
    cmd1 = '''
mkdir /mnt/temp{1..30} ;
curl http://download.ceph.com/tarballs/ceph_14.2.4.orig.tar.gz --output /mnt/temp1/ceph.tar.gz;
'''
    cmd2 = '''
for i in {1..30}; do cp /mnt/temp1/ceph.tar.gz /mnt/temp1/ceph_$i & done
'''
    cmd3 = '''
for i in {2..30};
do cp -r /mnt/temp1/* /mnt/temp$i/. &
done
'''
    pod.exec_bash_cmd_on_pod(command=cmd1)
    pod.exec_bash_cmd_on_pod(command=cmd2)
    for _ in range(iteration):
        pod.exec_bash_cmd_on_pod(command=cmd3)
