import os
import re
from typing import Tuple, List

DORIS_HOME = '../../../../'
cur_relative_path = ''
log_path = 'res.log'


def run_file(file_path: str):
    if os.path.isdir(file_path):
        return
    f = open(file_path, 'r').read()
    suite_names: List[str] = re.findall('suite\(\"[A-Za-z-_0-9]+\"\)', f)
    if len(suite_names) != 1:
        return
    suite_name: str = suite_names[0].replace('suite("', '').replace('")', '')
    print(suite_name)
    os.system(f'sh {DORIS_HOME}run-regression-test.sh --run -s {suite_name} > res.log')


def extractSql(log_str: str, index: int) -> str:
    log_str = log_str[index:]
    # we get the sql by find the next line of log(info/error/warn etc.)
    sql_end_index = min(log_str.find('ERROR'), log_str.find('WARN'), log_str.find('INFO')) \
        - len('\n2023-02-12 19:31:26.793 ')
    return log_str[:sql_end_index]


def check() -> Tuple[bool, str, str]:
    log_str = open(f'{DORIS_HOME}res.log').read()
    error_index = log_str.find('ERROR')
    if error_index == -1:
        return True, "", ""
    execute_index = log_str[:error_index].rfind('sql: ')
    error_sql = extractSql(log_str, execute_index + len('sql: '))
    error_str = log_str[error_index: error_index + log_str[error_index:].find('at ')]
    return False, error_sql, error_str


def adjustTest(file_path: str, error_sql: str):
    f = open(file_path, 'r')
    text = f.read()
    f.close()
    sql_index = text.find(error_sql)
    print(sql_index)
    prev_line_index = text[:sql_index].rfind('\n') + 1
    next_line_index = text[sql_index + len(error_sql):].find('\n') + sql_index + len(error_sql)
    print(prev_line_index, next_line_index)
    new_text = text[:prev_line_index] + "sql 'set enable_nereids_planner=false'\n"\
        + text[prev_line_index: next_line_index] + "\nsql 'set enable_nereids_planner=true'"\
        + text[next_line_index:]
    f = open(file_path, 'w')
    f.write(new_text)
    f.close()


log_f = open(log_path, 'w')


# def run(file_path: str):
#     for file_name in os.listdir(file_path):
#         file_path_name = os.path.join(file_path, file_name)
#         print(file_path_name)
#         if os.path.isdir(file_path_name):
#             run(file_path_name)
#         else:
#             while True:
#                 run_file(file_path_name)
#                 status, sql, log = check()
#                 if status:
#                     break
#                 log_f.write(log)
#                 adjustTest(file_path_name, sql)


run_file('../scalar_function/M.groovy')
status, sql, log = check()
print(status, sql, log)
# adjustTest('../scalar_function/M.groovy', sql)
