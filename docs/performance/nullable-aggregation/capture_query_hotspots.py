#!/usr/bin/env python3
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
"""Sample the isolated BE while repeatedly executing numeric_high_null1.
This is separate from the warmed timing cohorts; profile recording is disabled.
"""
import argparse
import json
from pathlib import Path
import subprocess
import time
import pymysql

p=argparse.ArgumentParser()
p.add_argument('label')
p.add_argument('backend_pid', type=int)
p.add_argument('--v2', choices=['true','false'], default='true')
p.add_argument('--output', type=Path, required=True)
p.add_argument('--case',default='numeric_high_null1')
p.add_argument('--queries-label')
a=p.parse_args()
root=Path(__file__).parent
query=json.loads((root/((a.queries_label or a.label)+'-queries.json')).read_text())['sql'][a.case]
c=pymysql.connect(host='127.0.0.1',port=22901,user='root',database='nullable_migration')
s=c.cursor()
for setting in ['enable_profile=false','parallel_pipeline_task_num=4','agg_phase=2',
                'enable_sql_cache=false','enable_aggregate_function_null_v2='+a.v2]:
    s.execute('set '+setting)
s.execute(query)
s.fetchall()
with a.output.with_suffix('.record.log').open('w') as log:
    proc=subprocess.Popen(['perf','record','--per-thread','--no-inherit','-m','2','-F','199','-e','cycles:u','-g','-p',str(a.backend_pid),
        '-o',str(a.output),'--','sleep','8'],stdout=log,stderr=subprocess.STDOUT)
    time.sleep(.25)
    queries=0
    while proc.poll() is None:
        s.execute(query)
        s.fetchall()
        queries+=1
    assert proc.returncode == 0, a.output.with_suffix('.record.log').read_text()
with (root/(a.label+'-query-hotspots.txt')).open('w') as log:
    subprocess.run(['perf','report','--stdio','--no-children','--sort=symbol','-i',str(a.output)],
                   stdout=log,stderr=subprocess.STDOUT,check=True)
(root/(a.label+'-query-hotspot-workload.json')).write_text(json.dumps(
    {'label':a.label,'case':a.case,'completed_queries':queries,'sample_seconds':8,
     'event':'cycles:u','frequency_hz':199,'scope':'existing warmed BE threads; per-thread buffers; no inheritance','sql':query},indent=2)+'\n')
print(a.label,'query CPU samples collected',queries)
