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
from simulator.IF_2D_getter import IF_2D_getter
from simulator.IF_3D_getter import IF_3D_getter
from simulator.IF_4D_getter import IF_4D_getter
from simulator.tester import tester
import os

scheduler = Scheduler()
app = Flask(__name__)

# 0 : random,
# 1 : algorithm 1,
# 2 : algorithm 2
algorithm = 2

@app.route('/request', methods=["GET"])
def proxy_to_redirect():
    content = request.get_json(silent=True)
    currentTime = time.time()
    if algorithm == 0:
        optimalURL, idleCpu, idleContainer = scheduler.getOptimalServiceRandom(content['workload_type']);
    elif algorithm == 1:
        print('nothing')
    elif algorithm == 2:
        optimalURL, idleCpu, idleContainer, job_uid = scheduler.algorithm_2(content['workload_type'], currentTime);

    logger.info("Host : %s, Container : %s, CPU : %sth, workload: %s" %
                (idleCpu['host'], idleContainer['id'].replace('\n',''), idleCpu['cpu'], content['workload_type']))

    # to optimalURL, priority and cpu information should be added.
    output = get(optimalURL, json={"priority":"None", "cpu":idleCpu['cpu'], "start_time":currentTime}).content.decode('utf-8')

    # After finish, restore 'cpu_status' and 'container_discovery'
    CpuStatusDAO().updateCpuStatus('idle', idleCpu['host'], idleCpu['cpu'])
    DiscoveryDAO().updateContainerStatus('stopped', idleContainer['id'])
    ExpectedEndTimeDAO().deleteData(job_uid)

    print('output : ' + output)

    return jsonify({'status': json.loads(output)})

@app.route('/initialize', methods=['PUT'])
def initialize():
    workloadList = WorkloadDAO().getAll()
    for workload in workloadList:
        optimalURL, elapsedTime, idleCpu, idleContainer = scheduler.getOptimalServiceRandom(workload['workload_name']);
        logger.info("Host : %s, Container : %s, CPU : %sth" %
                    (idleCpu['host'], idleContainer['id'].replace('\n', ''), idleCpu['cpu']))

        start_time = time.time()
        output = get(optimalURL, json={"priority": "None", "cpu": idleCpu['cpu']}).content.decode('utf-8')
        elapsedTime = time.time() - start_time

        ExpectedTimeDAO().updateExpectedTime(workload['workload_name'], elapsedTime)
        print('elapsed time for %s : %s' % (workload, elapsedTime))

        # After finish, restore 'cpu_status' and 'container_discovery'
        CpuStatusDAO().updateCpuStatus('idle', idleCpu['host'], idleCpu['cpu'])
        DiscoveryDAO().updateContainerStatus('stopped', idleContainer['id'])

    # TODO : execute all each workloads and get taken time
    return jsonify({'status': 'success'}), 200

if __name__ == '__main__':

    tester().test_3d  ()
    # data = IF_3D_getter().get_3d_IF()
    # print(data)

    # with open(os.path.dirname(__file__) + '/simulator/data', 'w') as f:
    #     f.write(str(data))
    # app.run(host='0.0.0.0', port=80)