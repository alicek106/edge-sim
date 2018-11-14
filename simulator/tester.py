from dao.DiscoveryDao import DiscoveryDAO
from dao.CpuStatusDao import CpuStatusDAO
from flask import Flask
from requests import get
from flask import jsonify
from Scheduler import Scheduler
import json
from flask import request
from logger.Logger import logger
from dao.WorkloadDao import WorkloadDAO, ExpectedTimeDAO, ExpectedEndTimeDAO
import time
from itertools import combinations_with_replacement
import threading
from pymongo import MongoClient
import pymongo
import ast
import os
from threading import Lock

mutex = Lock() # Mutex 매우 중요!!! 2018. 10. 28

class tester():
    __workload_list = None
    __workload_combinations = None
    __origin_time_result = None
    __origin_time = {}
    __client = None
    __db = None
    __testbed_host = 'intel_worker2'

    __exec_result = {}
    def __init__(self):
        self.__workload_list = WorkloadDAO().getAll()
        self.__origin_time_result = ExpectedTimeDAO().getAll()
        for record in self.__origin_time_result:
            self.__origin_time[record['workload']] = float(record['expected_time'])

        self.__client = MongoClient('163.180.117.195', 27000)
        self.__db = self.__client['metrics']

    # start_time이 소수점 5번째짜리까지만 들어가도록 수정해야함 2018.11.01 22:07.
    def __request_workload_algo2(self, name):
        currentTime = round(time.time(),5 )
        mutex.acquire()
        optimalURL, idleCpu, idleContainer, job_uid = Scheduler().algorithm_2(name, currentTime);
        mutex.release()

        logger.info("Host : %s, Container : %s, CPU : %sth, workload: %s" %
                    (idleCpu['host'], idleContainer['id'].replace('\n', ''), idleCpu['cpu'], name))

        # to optimalURL, priority and cpu information should be added.
        output = get(optimalURL, json={"priority": "None", "cpu": idleCpu['cpu'], 'start_time':currentTime}).content.decode('utf-8')

        # After finish, restore 'cpu_status' and 'container_discovery'
        CpuStatusDAO().updateCpuStatus('idle', idleCpu['host'], idleCpu['cpu'])
        DiscoveryDAO().updateContainerStatus('stopped', idleContainer['id'])
        record = ExpectedEndTimeDAO().getExpectedEndTimeByUID(job_uid)
        self.__exec_result[float(record['uid'])] = float(record['end_time']) - float(record['start_time'])
        ExpectedEndTimeDAO().deleteData(job_uid)

    def __request_workload(self, name):
        logger.info('start %s' % name)
        optimalURL, elapsedTime, idleCpu, idleContainer = Scheduler().getOptimalServiceRandom(name);
        logger.info("Host : %s, Container : %s, CPU : %sth" %
                    (idleCpu['host'], idleContainer['id'].replace('\n', ''), idleCpu['cpu']))
        output = get(optimalURL, json={"priority": "None", "cpu": idleCpu['cpu']}).content.decode('utf-8')
        logger.info('end %s' % name)

        # After finish, restore 'cpu_status' and 'container_discovery'
        CpuStatusDAO().updateCpuStatus('idle', idleCpu['host'], idleCpu['cpu'])
        DiscoveryDAO().updateContainerStatus('stopped', idleContainer['id'])

    def test_2d(self):
        self.__workload_combinations = list(combinations_with_replacement(self.__workload_list, 2))
        for workload_combination in self.__workload_combinations:
            self.__exec_result = {}
            threads = []

            t1 = threading.Thread(target=self.__request_workload_algo2, args=([workload_combination[0]['workload_name']]))
            t2 = threading.Thread(target=self.__request_workload_algo2, args=([workload_combination[1]['workload_name']]))
            threads.append(t1)
            threads.append(t2)
            t1.start()
            time.sleep(4)
            t2.start()
            for th in threads:
                th.join()
            logger.info('%s, %s' % (workload_combination[0]['workload_name'], workload_combination[1]['workload_name']))

            # Get speculation time
            records = ExpectedEndTimeDAO().getExpectedEndTime(self.__testbed_host)
            for record in records:
                self.__exec_result[float(record['uid'])] = float(record['end_time']) - float(record['start_time'])
            # ExpectedEndTimeDAO().deleteAllData()

            # Get real data
            docs = self.__db[self.__testbed_host].find().sort('spend_time', pymongo.DESCENDING)
            for doc in docs:
                spend_time_real = doc['spend_time']
                spend_time_speculation = self.__exec_result[doc['start_time']]
                print('real : %f, speculation : %f \n' % (spend_time_real, spend_time_speculation))

            self.__db[self.__testbed_host].drop()


    def test_3d(self):
        self.__workload_combinations = list(combinations_with_replacement(self.__workload_list, 3))
        for workload_combination in self.__workload_combinations:
            self.__exec_result = {}
            threads = []

            t1 = threading.Thread(target=self.__request_workload_algo2, args=([workload_combination[0]['workload_name']]))
            t2 = threading.Thread(target=self.__request_workload_algo2, args=([workload_combination[1]['workload_name']]))
            t3 = threading.Thread(target=self.__request_workload_algo2, args=([workload_combination[2]['workload_name']]))

            threads.append(t1)
            threads.append(t2)
            threads.append(t3)

            t1.start()
            time.sleep(2)
            t2.start()
            time.sleep(2)
            t3.start()

            for th in threads:
                th.join()
            logger.info('%s, %s, %s' %
                        (workload_combination[0]['workload_name'],
                         workload_combination[1]['workload_name'],
                         workload_combination[2]['workload_name']))

            # Get speculation time
            # records = ExpectedEndTimeDAO().getExpectedEndTime(self.__testbed_host)
            # for record in records:
            #     self.__exec_result[float(record['uid'])] = float(record['end_time']) - float(record['start_time'])
            # # ExpectedEndTimeDAO().deleteAllData()

            # Get real data
            docs = self.__db[self.__testbed_host].find().sort('spend_time', pymongo.DESCENDING)
            for doc in docs:
                spend_time_real = doc['spend_time']
                spend_time_speculation = self.__exec_result[doc['start_time']]
                print('real : %f, speculation : %f' % (spend_time_real, spend_time_speculation))
            print('\n')
            self.__db[self.__testbed_host].drop()

if __name__ == '__main__':
    tester().test_3d()