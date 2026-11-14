# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Require complete test reports with no raw failures, regardless of CI mutes.

Run after the lane's existing completeness, exit-code and sanitizer checks.
This is an additional verdict; it never replaces those checks or edits reports.
"""
import argparse
from pathlib import Path
import xml.etree.ElementTree as ET


def check(paths):
    assert paths, 'No JUnit reports supplied'
    cases = []
    for path in paths:
        root = ET.parse(path).getroot()
        assert root.tag in ('testsuite', 'testsuites'), 'Unexpected JUnit root'
        cases.extend(root.iter('testcase'))
    assert cases, 'No test cases reported'
    failed = sum(case.find('failure') is not None or case.find('error') is not None
                 for case in cases)
    executed = sum(case.find('skipped') is None and case.get('result') != 'skipped'
                   and case.get('status') != 'notrun' for case in cases)
    assert executed, 'All test cases skipped'
    print('SOURCE_CI_VERDICT cases={} executed={} failures={}'.format(len(cases), executed, failed))
    return 1 if failed else 0


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('reports', type=Path, nargs='+')
    args = parser.parse_args()
    raise SystemExit(check(args.reports))
