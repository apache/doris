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
"""Run after upgrading the old prepare.sql fixture to execution version 20.
The fixture table must be empty and must have been created by version 19.
"""
import argparse
import atexit
import json
from pathlib import Path
import time
import urllib.request
import pymysql

parser = argparse.ArgumentParser()
group = parser.add_mutually_exclusive_group()
group.add_argument('--resume-compaction',action='store_true',help='Resume the two committed bitmap fixture inserts after a failed trigger.')
group.add_argument('--resume-mixed-compaction',action='store_true',help='Compact previously written old/new rowsets after the bitmap phase completed.')
args = parser.parse_args()

c = pymysql.connect(host='127.0.0.1', port=22901, user='root', database='nullable_migration')
s = c.cursor()
s.execute('set enable_agg_state=true')
records = {'sql': [], 'compaction': [], 'resumed_compaction': args.resume_compaction}
result_path = Path(__file__).parent/('mixed-old-new-rowsets-compaction.json' if args.resume_mixed_compaction else 'mixed-old-schema-compaction.json')
atexit.register(lambda: result_path.write_text(json.dumps(records,default=str,indent=2)+'\n'))
def sql(query):
    records['sql'].append(query)
    s.execute(query)
    rows = s.fetchall()
    records.setdefault('results', []).append(rows)
    return rows

def request(path, method='GET'):
    req = urllib.request.Request('http://127.0.0.1:21911'+path, method=method,
                                 headers={'Authorization': 'Basic cm9vdDo='})
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    return json.loads(opener.open(req, timeout=60).read())

def compact(table):
    records.setdefault('compaction_tables', []).append(table)
    rows = sql('show tablets from '+table)
    columns = [d[0] for d in s.description]
    tablets = [dict(zip(columns, row)) for row in rows]
    assert tablets, table
    for tablet in tablets:
        tid = tablet['TabletId']
        before = request(f'/api/compaction/show?tablet_id={tid}')
        records['compaction'].append(before)
        started = request(f'/api/compaction/run?tablet_id={tid}&compact_type=full', 'POST')
        records['compaction'].append(started)
        assert started['status'] == 'Success', started
        for attempt in range(120):
            status = request(f'/api/compaction/run_status?tablet_id={tid}')
            after = request(f'/api/compaction/show?tablet_id={tid}')
            if not status['run_status'] and len(after['rowsets']) < len(before['rowsets']):
                break
            time.sleep(.5)
        else:
            raise RuntimeError('compaction did not finish')
        records['compaction'].append(after)
        assert len(after['rowsets']) < len(before['rowsets']), (before, after)

def verify_bitmap_marker():
    assert sql('select count(*) from old_empty_bitmap_states')[0][0] == (2 if args.resume_compaction else 0)
    for batch in ([] if args.resume_compaction else [0, 1]):
        sql(f"""insert into old_empty_bitmap_states
          select 1,bitmap_intersect_state(if(v is null,bitmap_from_string('1,2'),null))
          from perf_input where id={batch}
          union all
          select 2,bitmap_intersect_state(if(v is null,null,bitmap_empty()))
          from perf_input where id={batch}""")
    check = "select g,bitmap_to_string(bitmap_intersect_merge(b)) from old_empty_bitmap_states group by g order by g"
    assert sql(check) == ((1, ''), (2, ''))
    compact('old_empty_bitmap_states')
    assert sql(check) == ((1, ''), (2, ''))
    sql("""insert into old_empty_bitmap_states
     select 1,bitmap_intersect_state(if(v is null,null,bitmap_from_string('1,2')))
     from perf_input where id=2
     union all
     select 2,bitmap_intersect_state(if(v is null,null,bitmap_from_string('1,2')))
     from perf_input where id=2""")
    assert sql(check) == ((1, '1,2'), (2, ''))

if not args.resume_mixed_compaction:
    verify_bitmap_marker()

# Existing valid old states ignore new NULL-only contributions; historical collapsed
# empty states remain actual empty values and cannot recover lost information.
if not args.resume_mixed_compaction:
    sql("""insert into old_states
     select cast(id%8 as int),sum_state(if(v is null,v,null)),avg_state(if(d is null,d,null)),
     min_state(if(s is null,s,null)),avg_weighted_state(if(v is null,cast(v as double),null),w),
     bitmap_intersect_state(if(v is null,bitmap_from_string('1,2'),null)),
     collect_set_state(if(s is null,s,null)),array_agg_state(s),map_agg_v2_state(cast(id as string),s)
     from perf_input where id<8""")
rows = sql("select g,sum_merge(s),avg_merge(a),bitmap_to_string(bitmap_intersect_merge(b)) from old_states group by g order by g")
assert all(row[3] == '1,2' for row in rows[:7]) and rows[7][3] == ''
all_states = """select g,sum_merge(s),avg_merge(a),min_merge(m),avg_weighted_merge(w),
 bitmap_to_string(bitmap_intersect_merge(b)),array_size(collect_set_merge(c)),
 array_size(array_agg_merge(ar)),map_size(map_agg_v2_merge(mp))
 from old_states group by g order by g"""
before_states = sql(all_states)
compact('old_states')
assert sql(all_states) == before_states
result_path.write_text(json.dumps(records,default=str,indent=2)+'\n')
print('Old-schema marker compaction and mixed old/new rowsets PASS')
