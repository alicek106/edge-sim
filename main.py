from flask import Flask, request
from subprocess import check_output, STDOUT
import json
from pymongo import MongoClient
import ast
import datetime, time
from config import config
import os
from dao.StatusDao import StatusDAO

## Global variables
current_milli_time = lambda: int(round(time.time() * 1000))
app = Flask(__name__)
client = MongoClient(config['mongo_url'], 27017)
db = client['metrics']
__statusDao = StatusDAO()

@app.route('/')
def request_job():

    start_time = current_milli_time()
    __statusDao.updateStatus('running', start_time, -1, config['host'], config['workload_name'])

    ####
    priority = request.args.get('priority')
    if priority == None:
        priority = -1
        output = check_output('bash /STREAM/run.sh',
                              shell=True, stderr=STDOUT)
    else:
        output = check_output('bash /STREAM/run.sh %s' % (priority),
                          shell=True, stderr=STDOUT)
    output = output.decode('utf-8').replace('\n', '')
    store_result(priority, output)
    ####

    end_time= current_milli_time()
    __statusDao.updateStatus('stopped', start_time, end_time, config['host'], config['workload_name'])

    return json.dumps({'status': 'success'})

def store_result(priority, result_name):
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
        data_dict['date'] = datetime.datetime.utcnow()
        data_dict['priority'] = priority
        db[config['host']].insert_one(data_dict)

    os.remove('/STREAM/%s' % result_name)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80)