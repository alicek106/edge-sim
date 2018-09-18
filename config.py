import json
import os

config = {}

config['workload_name'] = os.environ['WORKLOAD']
config['host'] = os.environ['HOST']
config['mongo_url'] = os.environ['MONGO_URL']

print('\n\n######## start simulation ##########')
print('host : %s' % config['host'])
print('workload_name : %s' % config['workload_name'])
print('mongo_url : %s' % config['mongo_url'])
print('######## start simulation ##########\n\n')