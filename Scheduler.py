from dao.DiscoveryDao import DiscoveryDAO
from dao.CpuStatusDao import CpuStatusDAO
import random
from threading import Lock
import paramiko, time
from dao.HostDao import HostDAO
mutex = Lock()

class Scheduler:
    __ssh_conneciton = {}

    def __init__(self):
        hostList = list(HostDAO().getAll())
        for host in hostList:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(host['host_name'])
            self.__ssh_conneciton[host['host_name']] = ssh

        self.discoveryDao = DiscoveryDAO()

    def getOptimalServiceRandom(self, workloadType):
        # 1. Get start time
        start_time = time.time()

        # 2. Acquire Mutex
        mutex.acquire()

        # 3 Select idle cpu from all hosts -> retrieve cpu, host information
        cpuList = list(CpuStatusDAO().getAvailableCpu().fetchall())
        idleCpu = random.choice(cpuList)

        # 4. Select executable container from corresponding cpu (host)
        containerList = list(DiscoveryDAO().getAvailableContainer(workloadType, idleCpu['host']).fetchall())
        idleContainer = random.choice(containerList)

        # 5. Change cgroup option
        ssh = self.__ssh_conneciton[idleCpu['host']]
        cmd = 'echo %s > /sys/fs/cgroup/cpuset/docker/%s/cpuset.cpus' % (idleCpu['cpu'], idleContainer['id'].replace('\n', ''))
        ssh.exec_command(cmd)

        # 6. Update database 'cpu_status' and 'container_discovery' to running
        CpuStatusDAO().updateCpuStatus('running', idleCpu['host'], idleCpu['cpu'])
        DiscoveryDAO().updateContainerStatus('running', idleContainer['id'])

        # 7. End
        optimalNodeURL = "http://%s:%s" % (idleCpu['host'], idleContainer['endpoint']);
        elapsedTime = time.time() - start_time
        mutex.release()
        return optimalNodeURL, elapsedTime, idleCpu, idleContainer

if __name__ == '__main__':
    optimalNodeURL, elapsedTime, idleCpu, idleContainer = Scheduler().getOptimalServiceRandom('workload4')
    print(idleCpu)
    print(idleContainer)