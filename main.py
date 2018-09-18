from dao.DiscoveryDao import DiscoveryDAO
from dao.CpuStatusDao import CpuStatusDAO
from flask import Flask
from requests import get
from flask import jsonify
from Scheduler import Scheduler
import json
from flask import request
from logger.Logger import logger

scheduler = Scheduler()
app = Flask(__name__)

@app.route('/request', methods=["POST"])
def proxy_to_redirect():
    content = request.get_json(silent=True)
    optimalURL, elapsedTime, idleCpu, idleContainer = scheduler.getOptimalServiceRandom(content['workload_type']);
    logger.info("Host : %s, Container : %s, CPU : %sth, workload: %s" %
                (idleCpu['host'], idleContainer['id'].replace('\n',''), idleCpu['cpu'], content['workload_type']))

    output = get(optimalURL).content.decode('utf-8')

    # After finish, restore 'cpu_status' and 'container_discovery'
    CpuStatusDAO().updateCpuStatus('idle', idleCpu['host'], idleCpu['cpu'])
    DiscoveryDAO().updateContainerStatus('stopped', idleContainer['id'])

    return jsonify({'status': json.loads(output)})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80)