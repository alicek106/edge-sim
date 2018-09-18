import json
import os

config = {}

with open(os.path.dirname(__file__) + '/config.json') as f:
    config = json.load(f)

config['mode'] = os.environ['MODE']
config['mysql']['host'] = os.environ['MYSQL_URL']
print('\n\n######## start simulation ##########')
print('mysql_host : %s' % config['mysql']['host'])

if config['mode'] is 1:
    print('mode : random scheduler')
elif config['mode'] is 2:
    print('mode : custom scheduler')

print('######## start simulation ##########\n\n')