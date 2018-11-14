from dao.DiscoveryDao import DiscoveryDAO
from dao.CpuStatusDao import CpuStatusDAO
import random
from threading import Lock
import paramiko, time
from dao.HostDao import HostDAO
from dao.WorkloadDao import WorkloadDAO, ExpectedTimeDAO, ExpectedEndTimeDAO
import os
import ast
mutex = Lock() # Mutex 매우 중요!!! 2018. 10. 28

class Scheduler:
    __ssh_conneciton = {}
    __IF = {}
    __origin_time = {}

    def __init__(self):
        self.__origin_time_result = ExpectedTimeDAO().getAll()
        for record in self.__origin_time_result:
            self.__origin_time[record['workload']] = float(record['expected_time'])

        with open(os.path.dirname(__file__) + '/simulator/data', 'r') as f:
            data = f.read()
            self.__IF = ast.literal_eval(data)

        hostList = list(HostDAO().getAll())
        for host in hostList:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(host['host_name'], port=7877, username='root')
            self.__ssh_conneciton[host['host_name']] = ssh

        self.discoveryDao = DiscoveryDAO()

    # 테스트용 (비교 대조군?)
    def getOptimalServiceRandom(self, workloadType, currentTime):
        # 1. Get start time
        start_time = currentTime

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


    # 알고리즘 2번째
    def algorithm_2(self, workloadType, currentTime):
        ####### step 1.  각 Host마다 새로운 Workload에 대한 연장 시간 계산
        expected_end_original_time = self.__origin_time[workloadType] + float(currentTime)
        hosts = list(HostDAO().getAll())
        hosts_IF = {}
        for host in hosts:
            hosts_IF[host['host_name']] = 0

        for host in hosts:
            running_job = sorted(ExpectedEndTimeDAO().getExpectedEndTime(host['host_name']), key=lambda k: k['end_time'], reverse=True)
            for index in range(0, len(running_job)):
                workload_list = []
                workload_list.append(workloadType)
                OvT = 0

                # running_job의 맨 처음 element에 대해서 수행. running_job이 하나만 있는 경우
                if index == 0 and index + 1 == len(running_job):
                    workload_list.append(running_job[0]['workload_type'])
                    workload_list.sort()
                    workload_list.append('target-' + workloadType)
                    IF_ = self.__IF[tuple(workload_list)]

                    ## If there is only one running job..
                    ## Type 1. 새로온 job의 예측 시간이 기존 job의 예측 종료시간보다 길 때
                    if expected_end_original_time > float(running_job[0]['end_time']):
                        OvT = (float(running_job[0]['end_time']) - float(currentTime))
                    ## Type 2. 새로운 job과 기존 job의 예측 종료시간 중, 일부만 겹쳐서 실행될 때
                    else:
                        OvT = self.__origin_time[workloadType]
                    hosts_IF[host['host_name']] = hosts_IF[host['host_name']] + OvT * IF_['IV'] * self.__origin_time[workloadType] / self.minTime(workload_list)
                    break;

                else:
                    for i in range(0, index + 1):
                        workload_list.append(running_job[i]['workload_type'])
                    workload_list.sort()
                    workload_list.append('target-' + workloadType)
                    IF_ = self.__IF[tuple(workload_list)]

                    ## 마지막 element인 경우
                    if index + 1 == len(running_job):
                        ## Type 6. 마지막 workload 조합보다 길 경우
                        if expected_end_original_time > float(running_job[index]['end_time']):
                            OvT = float(running_job[index]['end_time']) - float(currentTime)

                        ## Type 7. 마지막 workload 조합보다 작을 경우
                        elif expected_end_original_time < float(running_job[index]['end_time']):
                            OvT = self.__origin_time[workloadType]
                    else:
                        ## Type 3
                        if expected_end_original_time > float(running_job[index]['end_time']):
                            OvT = float(running_job[index]['end_time']) - float(running_job[index + 1]['end_time'])
                        ## Type 4
                        elif expected_end_original_time < float(running_job[index]['end_time']) and \
                                expected_end_original_time > float(running_job[index + 1]['end_time']):
                            OvT = float(expected_end_original_time) - float(running_job[index + 1]['end_time'])
                        ## Type 5. 새로운 job은 running_job[0]에 의해서만 영향받는 일이 없음.
                        else:
                            continue

                hosts_IF[host['host_name']] = hosts_IF[host['host_name']] + OvT * IF_['IV'] * self.__origin_time[workloadType] / self.minTime(workload_list)
        #######

        mutex.acquire()

        # We have to update other workloads' expected end time
        # 함수 반환값 Result는 [{uid, host, workload_type, start_time, end_time, extended_time, class(?)}] 형식의, List of dict로.
        existing_job_extended_time = {}
        for key, value in hosts_IF.items():
            running_job = sorted(ExpectedEndTimeDAO().getExpectedEndTime(key), key=lambda k: k['end_time'], reverse=True)
            existing_job_extended_time[key] = self.algorithm_2_get_update_value(workloadType, currentTime, running_job)  # 새로운 record가 들어가기 전 update를 해야 함.

        # 일단 임시로.. 새로운 Workload Job이 가장 적은 연장 시간을 갖는 경우의 Host를 선택해 할당하도록 함.
        # TODO : 2018. 10. 30. 20:55 -> 나중에는, Running Job들과 새로 들어온 Job들의 모든 성능 감소치를 고려해 최적의 호스트 선택.
        optimalHost = self.getOptimalHost(hosts_IF)
        if existing_job_extended_time[optimalHost] is not None: # None인 경우 : 기존에 실행중인 Job이 한개도 없을 때, 호스트가 비어있을 때
            for record in existing_job_extended_time[optimalHost]:
                ExpectedEndTimeDAO().updateEndTime(record['uid'], record['end_time'] + record['extended_time'])
        # TODO : optimalHost가 확정되면, 해당 호스트에서 실행 중이던 Job들의 expected_end_time을 update 해야함 : 완료 (윗라인)



        ######## step 2.  해당 Host에서 사용 가능한 컨테이너, CPU 등 얻어오기. 이 아랫쪽은 아무~ 상관할 필요가 없음. 정해진대로 함.
        # 2.1 Select idle cpu from all hosts -> retrieve cpu, host information
        idleCpu = list(CpuStatusDAO().getAvailableCpuFromHost(optimalHost))[0]

        # 2.2 Select executable container from corresponding cpu (host)
        containerList = list(DiscoveryDAO().getAvailableContainer(workloadType, optimalHost).fetchall())
        idleContainer = random.choice(containerList)

        # 2.3 Change cgroup option
        ssh = self.__ssh_conneciton[optimalHost]
        cmd = 'echo %s > /sys/fs/cgroup/cpuset/docker/%s/cpuset.cpus' % (idleCpu['cpu'], idleContainer['id'].replace('\n', ''))
        ssh.exec_command(cmd)

        # 2.4 Update database 'cpu_status' and 'container_discovery' to running, insertExpectedEndTime
        CpuStatusDAO().updateCpuStatus('running', optimalHost, idleCpu['cpu'])
        DiscoveryDAO().updateContainerStatus('running', idleContainer['id'])
        job_uid = currentTime
        ExpectedEndTimeDAO().insertExpectedEndTime(job_uid, optimalHost, workloadType, currentTime, currentTime + self.__origin_time[workloadType] + hosts_IF[optimalHost])

        # 2.5 End
        optimalNodeURL = "http://%s:%s" % (optimalHost, idleContainer['endpoint']);
        mutex.release()

        return optimalNodeURL, idleCpu, idleContainer, job_uid


    # 알고리즘 2번째를 위한 업데이트 함수. 새로운 Job이 들어왔을 때,
    # 해당 호스트에서 실행 중이던 다른 Job들의 소요 시간을 증가시킨다. 아래 함수는 그 값을 얻기 위한 함수.
    def algorithm_2_get_update_value(self, workloadType, currentTime, runningJobs):
        return_list = []
        expected_end_original_time = self.__origin_time[workloadType] + float(currentTime)

        if len(runningJobs) is 0:
            return
        elif len(runningJobs) is 1:
            OvT = 0
            record = runningJobs[0]
            workload_list = []
            workload_list.append(workloadType)
            workload_list.append(record['workload_type'])
            workload_list.sort()
            workload_list.append('target-' + record['workload_type'])
            IF_ = self.__IF[tuple(workload_list)]

            if record['end_time'] < expected_end_original_time:
                OvT = record['end_time'] - currentTime
            elif record['end_time'] > expected_end_original_time:
                OvT = self.__origin_time[workloadType]
            added_time = OvT * IF_['IV'] * self.__origin_time[workloadType] / self.minTime(workload_list)
            record['extended_time'] = added_time
            return_list.append(record)
            return return_list

        else:
            # extended Time을 0으로 설정해서 초기화한단다.
            for job in runningJobs:
                job['extended_time'] = 0
                return_list.append(job)

            for index in range(0, len(runningJobs)):
                # For the first element of runningJobs
                if index is 0:
                    workload_list = [workloadType, runningJobs[0]['workload_type']]
                    workload_list.sort()
                    workload_list.append('target-' + runningJobs[0]['workload_type'])
                    IF_ = self.__IF[tuple(workload_list)]
                    if runningJobs[index]['end_time'] < expected_end_original_time:
                        OvT = runningJobs[0]['end_time'] - runningJobs[1]['end_time']
                    elif runningJobs[index]['end_time'] > expected_end_original_time and \
                            runningJobs[index+1]['end_time'] < expected_end_original_time:
                        OvT = expected_end_original_time - runningJobs[1]['end_time']
                    else:
                        OvT = 0
                    extended_time = OvT * IF_['IV'] * self.__origin_time[runningJobs[0]['workload_type']] / self.minTime(workload_list)
                    return_list[index]['extended_time'] = return_list[index]['extended_time'] + extended_time

                else:
                    OvT = 0

                    ## 마지막 element인 경우
                    if index + 1 == len(runningJobs):
                        if runningJobs[index]['end_time'] > expected_end_original_time:
                            OvT = runningJobs[index]['end_time'] - expected_end_original_time
                        elif runningJobs[index]['end_time'] < expected_end_original_time:
                            OvT = runningJobs[index]['end_time'] - currentTime
                    ## 일반적인 경우
                    else:
                        if runningJobs[index]['end_time'] < expected_end_original_time:
                            OvT = runningJobs[index]['end_time'] - runningJobs[index+1]['end_time']
                        elif runningJobs[index]['end_time'] > expected_end_original_time and \
                                runningJobs[index + 1]['end_time'] < expected_end_original_time:
                            OvT = expected_end_original_time - runningJobs[index+1]['end_time']
                        else:
                            OvT = 0

                    # 미리 OvT를 구하고 아래 for문을 수행
                    for index_2 in range(0, index + 1):
                        workload_list_after = []
                        workload_list_after.append(workloadType)
                        for i in range(0, index + 1):
                            workload_list_after.append(runningJobs[i]['workload_type'])
                        workload_list_after.sort()
                        workload_list_after.append('target-' + runningJobs[index_2]['workload_type'])

                        workload_list_before = []
                        for i in range(0, index + 1):
                            workload_list_before.append(runningJobs[i]['workload_type'])
                        workload_list_before.sort()
                        workload_list_before.append('target-' + runningJobs[index_2]['workload_type'])

                        IF_before = self.__IF[tuple(workload_list_before)]
                        extended_time_before = self.minTime(workload_list_after) * \
                                               IF_before['IV'] * self.__origin_time[runningJobs[index_2]['workload_type']] / self.minTime(workload_list_before)

                        IF_after = self.__IF[tuple(workload_list_after)]
                        extended_time_after = self.minTime(workload_list_after) * \
                                              IF_after['IV'] * self.__origin_time[runningJobs[index_2]['workload_type']] / self.minTime(workload_list_after)

                        extended_time_value = extended_time_after - extended_time_before # self.minTime(workload_list_after) 와 함께 쓰임
                        extended_time_final = extended_time_value * OvT / self.minTime(workload_list_after)
                        return_list[index_2]['extended_time'] = return_list[index_2]['extended_time'] + extended_time_final

            return return_list

    ## Workload 중 최소 소요 시간 골라내기 (original Time에서)
    def minTime(self, params):
        min = 999999
        for param in params:
            if 'target' in param:
                return min
            if self.__origin_time[param] < min:
                min = self.__origin_time[param]
        return min

    ## 값이 최소인 Host 골라내기 or 다른 Job의 연장 시간을 고려해서 최적의 호스트 선정
    def getOptimalHost(self, params):
        min = 99999
        minHost = None
        for key in params:
            if params[key] < min:
                minHost = key
                min = params[key]
        return minHost

if __name__ == '__main__':
    # print(Scheduler().algorithm_2('img-dnn', 0))
    # print(Scheduler().algorithm_2('img-dnn', 0))
    # print(Scheduler().algorithm_2('img-dnn', 0))
    # print(Scheduler().algorithm_2('img-dnn', 0.1))

    # print(Scheduler().algorithm_2('sphinx', 4))
    # print(Scheduler().algorithm_2('img-dnn', 0))
    # print(Scheduler().algorithm_2('img-dnn', 0))
    print(Scheduler().algorithm_2('xapian', 8))

    # print(Scheduler().algorithm_2('moses', 9))
    # optimalNodeURL, elapsedTime, idleCpu, idleContainer = Scheduler().getOptimalServiceRandom('workload4')