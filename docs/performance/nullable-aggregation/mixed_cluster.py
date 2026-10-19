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
"""Read/write compatibility in an isolated 2-BE cluster pinned to execution version 19.
Use prepare once, then verify with the old FE and new FE, finally restore.
All replica status changes affect only these disposable fixture tables.
"""
import argparse
from collections import defaultdict
import gzip
import json
import math
from pathlib import Path
import re
import time
import urllib.request
import pymysql

p=argparse.ArgumentParser()
p.add_argument('action',choices=['prepare','append','verify','restore'])
p.add_argument('--label',default='mixed-old-fe')
p.add_argument('--new-heartbeat-port',type=int,required=True)
a=p.parse_args()
root=Path(__file__).parent
c=pymysql.connect(host='127.0.0.1',port=22901,user='root',database='nullable_migration')
s=c.cursor()
commands=[]
def sql(text):
    commands.append(text)
    s.execute(text)
    return s.fetchall()

def mapped(text):
    rows=sql(text)
    return [dict(zip([d[0] for d in s.description],row)) for row in rows]

for setting in ['enable_agg_state=true','enable_profile=true','profile_level=2',
                'parallel_pipeline_task_num=4','agg_phase=2','enable_sql_cache=false']:
    sql('set '+setting)
assert sql("admin show frontend config like 'be_exec_version'")[0][1] == '19'
backends=mapped('show backends')
assert len(backends)==2 and all(str(b['Alive']).lower()=='true' for b in backends)
old=next(b for b in backends if int(b['HeartbeatPort'])==22921)
new=next(b for b in backends if int(b['HeartbeatPort'])==a.new_heartbeat_port)
sql('admin set frontend config ("disable_tablet_scheduler"="true")')
def insert_input(offset,count):
    sql(f"""insert into mixed_upgrade_input select number+{offset},cast((number+{offset})%8 as int),
      if((number+{offset})%8=7 or (number+{offset})%5=0,null,(number+{offset})%1000),
      if((number+{offset})%8=7 or (number+{offset})%5=0,null,cast((number+{offset})%1000 as decimal(27,9))),
      if((number+{offset})%11=0,null,cast((number+{offset})%13+1 as double)),
      if((number+{offset})%5=0,null,concat(repeat('wide-',128),cast((number+{offset})%127 as string))),
      bitmap_from_string('1,2') from numbers("number"="{count}")""")

def insert_states(begin,end):
    sql(f"""insert into mixed_upgrade_states select g,sum_state(v),avg_state(d),count_state(v),
          avg_weighted_state(cast(v as double),w),min_state(s),
          bitmap_intersect_state(if(id<0,null,b)),collect_set_state(s),array_agg_state(s),
          map_agg_v2_state(cast(id as string),s) from mixed_upgrade_input
          where id>={begin} and id<{end}""")

if a.action=='prepare':
    sql('drop table if exists mixed_upgrade_input')
    sql("""create table mixed_upgrade_input(
      id int not null,g int not null,v bigint,d decimal(27,9),w double,s string,b bitmap not null)
      duplicate key(id,g) distributed by hash(id) buckets 8
      properties("replication_num"="2","disable_auto_compaction"="true")""")
    insert_input(0,32768)
    sql('drop table if exists mixed_upgrade_states')
    sql("""create table mixed_upgrade_states(g int,
      s agg_state<sum(bigint)> generic,a agg_state<avg(decimal(27,9))> generic,
      ct agg_state<count(bigint)> generic,w agg_state<avg_weighted(double,double)> generic,
      m agg_state<min(string)> generic,b agg_state<bitmap_intersect(bitmap)> generic,
      c agg_state<collect_set(string)> generic,ar agg_state<array_agg(string)> generic,
      mp agg_state<map_agg_v2(string not null,string)> generic)
      aggregate key(g) distributed by hash(g) buckets 4
      properties("replication_num"="2","disable_auto_compaction"="true")""")
    # Version 19 cannot encode a NULL-only bitmap state separately from an empty
    # bitmap. Use nullable-typed valid bitmaps for this legacy interop fixture;
    # version-20 NULL markers are exercised by verify_upgrade.py.
    for batch in [0,1]:
        insert_states(batch*16384,(batch+1)*16384)
    # Every tablet has both replicas. Make alternate owners ineligible for reads
    # while scheduling is disabled, so scans must execute on both binary versions.
    changed=[]
    for table in ['mixed_upgrade_input','mixed_upgrade_states']:
        tablets=defaultdict(list)
        for row in mapped('show tablets from '+table):
            tablets[row['TabletId']].append(row)
        for index,(tid,rows) in enumerate(sorted(tablets.items())):
            assert len(rows)==2
            bad=new['BackendId'] if index%2==0 else old['BackendId']
            sql(f'admin set replica status properties("tablet_id"="{tid}","backend_id"="{bad}","status"="bad")')
            changed.append({'tablet_id':tid,'backend_id':bad,'table':table})
    (root/'mixed-replicas.json').write_text(json.dumps({'changed':changed,'backends':backends,
        'commands':commands},default=str,indent=2)+'\n')
elif a.action=='append':
    # Add a disjoint batch through the new FE at version 19, writing both replicas.
    # Restore replica health before this action; verify constrains reads afterwards.
    insert_input(32768,16384)
    insert_states(32768,49152)
    (root/(a.label+'-mixed-write.json')).write_text(json.dumps(
        {'backends':backends,'commands':commands},default=str,indent=2)+'\n')
elif a.action=='restore':
    for row in json.loads((root/'mixed-replicas.json').read_text())['changed']:
        sql(f'admin set replica status properties("tablet_id"="{row["tablet_id"]}","backend_id"="{row["backend_id"]}","status"="ok")')
    sql('admin set frontend config ("disable_tablet_scheduler"="false")')
else:
    # A BE restart may report healthy replicas again; constrain reads for this run.
    for row in json.loads((root/'mixed-replicas.json').read_text())['changed']:
        sql(f'admin set replica status properties("tablet_id"="{row["tablet_id"]}","backend_id"="{row["backend_id"]}","status"="bad")')
    raw="""select g,sum(v),avg(d),count(v),avg_weighted(cast(v as double),w),min(s),
      bitmap_to_string(bitmap_intersect(if(id<0,null,b))),array_size(collect_set(s)),
      array_size(array_agg(s)),map_size(map_agg_v2(cast(id as string),s))
      from mixed_upgrade_input where g<7 group by g order by g"""
    stored="""select g,sum_merge(s),avg_merge(a),count_merge(ct),avg_weighted_merge(w),min_merge(m),
      bitmap_to_string(bitmap_intersect_merge(b)),array_size(collect_set_merge(c)),
      array_size(array_agg_merge(ar)),map_size(map_agg_v2_merge(mp))
      from mixed_upgrade_states where g<7 group by g order by g"""
    opener=urllib.request.build_opener(urllib.request.ProxyHandler({}))
    results={}
    for flag in ['false','true']:
        sql('set enable_aggregate_function_null_v2='+flag)
        for name,text in [('raw',raw),('stored',stored)]:
            rows=sql(text)
            qid=sql('select last_query_id()')[0][0]
            req=urllib.request.Request('http://127.0.0.1:21901/api/profile/text?query_id='+qid,
                                        headers={'Authorization':'Basic cm9vdDo='})
            for attempt in range(30):
                profile=opener.open(req,timeout=60).read().decode()
                if 'Profile Completion State: COMPLETE' in profile:break
                time.sleep(.1)
            assert 'port:22921' in profile and 'port:'+str(a.new_heartbeat_port) in profile, 'both BE profiles required'
            with gzip.open(root/f'{a.label}-{flag}-{name}.profile.txt.gz','wt') as f:f.write(profile)
            results[flag+'_'+name]=rows
    expected=results['false_raw']
    for name,rows in results.items():
        assert len(rows)==len(expected)
        for wanted,actual in zip(expected,rows):
            for x,y in zip(wanted,actual):
                assert math.isclose(x,y,rel_tol=1e-9,abs_tol=1e-9) if isinstance(x,float) else x==y,(name,x,y)
    (root/(a.label+'-mixed-cluster.json')).write_text(json.dumps({'backends':backends,
        'commands':commands,'results':results,'profile_heartbeat_ports':[22921,a.new_heartbeat_port]},
        default=str,indent=2)+'\n')
print(a.action,a.label,'PASS')
