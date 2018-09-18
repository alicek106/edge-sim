from flask import Flask, request
from subprocess import check_output, STDOUT
import json
from pymongo import MongoClient
import ast
import datetime, time
from config import config
import os
from logger.Logger import logger

## Global variables
current_milli_time = lambda: int(round(time.time() * 1000))
app = Flask(__name__)
client = MongoClient(config['mongo_url'], 27017)
db = client['metrics']

@app.route('/')
def request_job():
    start_time = time.time()
    content = request.get_json(silent=True)
    priority = content['priority']
    cpu = content['cpu']
    logger.info('Priority : %s, CPU : %s' % (priority, cpu))

    if priority == 'None':
        print('Priority is None.')
        priority = -1
        output = check_output('bash /STREAM/run.sh', shell=True, stderr=STDOUT)
    else:
        output = check_output('bash /STREAM/run.sh %s' % (priority), shell=True, stderr=STDOUT)
    output = output.decode('utf-8').replace('\n', '')
    end_time = time.time()

    store_result(priority, output, cpu, start_time, end_time)
    return json.dumps({'status': 'success'})

def store_result(priority, result_name, cpu, start_time, end_time):
    data_dict = {}
    with open('/STREAM/%s' % result_name, 'r') as fp:
        # line from 5 ~ 16 will be stored in mongo table
        for i, line in enumerate(fp):
            # data metrics
            if i >= 5 and i <= 16:
                data = ' '.join(line.split('#')[0].split('      ')).split()
                data_dict[data[1]] = ast.literal_eval(data[0])

            # Spend time
            if i == 18:
                data = ' '.join(line.split('#')[0].split(' ')).split()[0]
                data_dict['spend_time'] = ast.literal_eval(data)

        data_dict['workload_name'] = config['workload_name']
        data_dict['host'] = config['host']
        data_dict['start_time'] = start_time
        data_dict['end_time'] = end_time
        # data_dict['date'] = datetime.datetime.utcnow()
        data_dict['priority'] = priority
        data_dict['cpu'] = cpu
        db[config['host']].insert_one(data_dict)

    os.remove('/STREAM/%s' % result_name)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80)