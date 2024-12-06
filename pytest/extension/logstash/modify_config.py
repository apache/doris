import env
import os

# 命令行参数代替

WORKERS = 1
BATCH_SIZE = 100
BATCH_DELAY = 3000

logstash_config = f'{env.LOGSTASH_HOME}/config/logstash.yml'
logstash_config_bak = f'{logstash_config}.bak'

if not os.path.exists(logstash_config_bak):
    status = os.system(f'cp {logstash_config} {logstash_config_bak}')
    if status != 0:
        raise Exception(f'Failed to backup logstash config file: {logstash_config}')

status = os.system(f'cp {logstash_config_bak} {logstash_config}')
if status != 0:
    raise Exception(f'Failed to restore logstash config file: {logstash_config}')

with open(logstash_config, 'a') as f:
    f.write(f'pipeline.workers: {WORKERS}\n')
    f.write(f'pipeline.batch.size: {BATCH_SIZE}\n')
    f.write(f'pipeline.batch.delay: {BATCH_DELAY}\n')
    f.write('config.support_escapes: true\n')
