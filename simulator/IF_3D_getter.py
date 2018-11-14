from dao.DiscoveryDao import DiscoveryDAO
from dao.CpuStatusDao import CpuStatusDAO
from flask import Flask
from requests import get
from flask import jsonify
from Scheduler import Scheduler
import json
from flask import request
from logger.Logger import logger
from dao.WorkloadDao import WorkloadDAO, ExpectedTimeDAO
import time
from itertools import combinations_with_replacement
import threading
from pymongo import MongoClient
import pymongo
import ast
import os

class IF_3D_getter():
    __workload_list = None
    __workload_combinations = None
    __origin_time_result = None
    __origin_time = {}
    __sem = None
    __IF = {}
    __client = None
    __db = None
    __testbed_host = 'intel_worker2'

    def __init__(self):
        self.__workload_list = WorkloadDAO().getAll()
        self.__workload_combinations = list(combinations_with_replacement(self.__workload_list, 3))
        self.__sem = threading.Semaphore(3)
        self.__origin_time_result = ExpectedTimeDAO().getAll()
        for record in self.__origin_time_result:
            self.__origin_time[record['workload']] = float(record['expected_time'])

        self.__client = MongoClient('163.180.117.195', 27000)
        self.__db = self.__client['metrics']
        with open(os.path.dirname(__file__) + '/data', 'r') as f:
            data = f.read()
            self.__IF = ast.literal_eval(data)

    def __request_workload(self, name):
        currentTime = round(time.time(), 5)
        logger.info('start %s' % name)

        self.__sem.acquire()
        optimalURL, elapsedTime, idleCpu, idleContainer = Scheduler().getOptimalServiceRandom(name, currentTime);
        logger.info("Host : %s, Container : %s, CPU : %sth" %
                    (idleCpu['host'], idleContainer['id'].replace('\n', ''), idleCpu['cpu']))
        output = get(optimalURL, json={"priority": "None", "cpu": idleCpu['cpu'], "start_time":currentTime}).content.decode('utf-8')
        self.__sem.release()
        logger.info('end %s' % name)

        # After finish, restore 'cpu_status' and 'container_discovery'
        CpuStatusDAO().updateCpuStatus('idle', idleCpu['host'], idleCpu['cpu'])
        DiscoveryDAO().updateContainerStatus('stopped', idleContainer['id'])

    def __get_beta(self, docs, index_workload, doc_param):
        b = 0;
        if index_workload == 0:
            for index in range(index_workload, len(docs) - 1):  # index : 0, 1
                if index + 2 == len(docs):  # Index start in 0!
                    return b

                workload_list = []
                for i in range(0, index + 2):
                    workload_list.append(docs[i]['workload_name'])
                workload_list.sort()
                workload_list.append('target-' + doc_param['workload_name'])
                IF_ = self.__IF[tuple(workload_list)]

                b = b + IF_['IV'] * self.__origin_time[doc_param['workload_name']] * \
                    ((docs[index + 1]['spend_time'] - docs[index + 2]['spend_time']) / IF_['minTime'])

            return b;

        elif index_workload == len(docs)-1:
            return b

        else:
            for index in range(index_workload, len(docs) - 1):
                workload_list = []
                for i in range(0, index + 1):
                    workload_list.append(docs[i]['workload_name'])
                workload_list.sort()
                workload_list.append('target-' + doc_param['workload_name'])
                IF_ = self.__IF[tuple(workload_list)]

                b = b + IF_['IV'] * self.__origin_time[doc_param['workload_name']] * \
                    ((docs[index]['spend_time'] - docs[index + 1]['spend_time']) / IF_['minTime'])

            return b;


    def get_3d_IF(self):
        for workload_combination in self.__workload_combinations:
            threads = []
            t1 = threading.Thread(target=self.__request_workload, args=([workload_combination[0]['workload_name']]))
            t2 = threading.Thread(target=self.__request_workload, args=([workload_combination[1]['workload_name']]))
            t3 = threading.Thread(target=self.__request_workload, args=([workload_combination[2]['workload_name']]))
            threads.append(t1)
            threads.append(t2)
            threads.append(t3)
            t1.start()
            t2.start()
            t3.start()

            for th in threads:
                th.join()
            logger.info('All threads ended')

            docs = self.__db[self.__testbed_host].find().sort('spend_time', pymongo.DESCENDING)
            doc_list = []
            workload_input_list = []
            for doc in docs:
                workload_input_list.append(doc['workload_name'])
                doc_list.append(doc)
                print(doc)

            workload_input_list.sort()

            for index, doc in enumerate(doc_list):
                b = self.__get_beta(doc_list, index, doc)
                IV = (doc['spend_time'] - self.__origin_time[doc['workload_name']] - b) / self.__origin_time[doc['workload_name']]
                minTime = doc_list[len(doc_list) - 1]['spend_time']
                workload_input_list.append('target-' + doc['workload_name'])
                self.__IF[tuple(workload_input_list)] = {'minTime': minTime, 'IV': IV}
                print(str(tuple(workload_input_list)) + ' / ' + str({'minTime': minTime, 'IV': IV}))
                workload_input_list.pop(len(workload_input_list) - 1)
            self.__db[self.__testbed_host].drop()
        return self.__IF
