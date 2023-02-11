import os
import re

DORIS_HOME = '../../../../'
cur_relative_path = ''


def load():
    print("start load data")
    exit_code = os.system(f'sh {DORIS_HOME}run-regression-test.sh --run -s load > res.log')
    if exit_code != 0:
        exit(exit_code)
    print("end load data")


# def run_file(file_path: str):
#     if os.path.isdir(file_path):
#         return
#     f = open(file_path, 'r').read()
#     suite_names = re.findall('suite\(\"[A-Za-z-_0-9]+\"\)', f)
#     if len(suite_names) != 0:
#         return
#     suite_name = suite_names[0]
#
#
# def check() -> bool:
#     return False
#
#
# def closeTest():
#     return
#
#
# def recover():
#     return
#
#
# def collectException():
#     return
#
#
# def genAnswer():
#     return

load()
