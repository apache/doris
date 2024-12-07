import os
import env

case_list = [
    'columns',
    'csv_basic',
    'csv_column_separator',
    # 'csv_enclose_escape', # not supported
    'csv_enclose',
    'group_commit_async',
    'group_commit_sync',
    'json_basic',
    'json_fuzzy_parse',
    'json_mapping',
    'json_nested',
    'json_num_as_string',
    'json_paths',
    'json_root',
    'max_filter_ratio',
    'strict_mode',
    'timezone',
]

def execute(cmd: str):
    status = os.system(cmd)
    if status != 0:
        raise Exception(f'Failed to execute command: {cmd}')

execute('python3 build.py')
execute('python3 install.py')
execute('python3 modify_config.py')
execute('python3 start.py')
for case in case_list:
    execute(f'python3 run.py --case {case}')
