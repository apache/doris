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
"""Run against the isolated SF1 + prepare.sql dataset. Requires pymysql.
Run once per build/mode, after builds and microbenchmarks have stopped.
"""
import argparse
import gzip
import hashlib
import json
from pathlib import Path
import time
import urllib.request
import pymysql

p = argparse.ArgumentParser()
p.add_argument('label')
p.add_argument('--v2', choices=['true', 'false'], default='true')
p.add_argument('--cases', default='')
p.add_argument('--append', action='store_true')
p.add_argument('--repetitions', type=int, default=5)
p.add_argument('--warmup-repetitions', type=int, default=5)
p.add_argument('--port', type=int, default=22901)
p.add_argument('--http-port', type=int, default=21901)
p.add_argument('--output', type=Path, default=Path(__file__).parent)
a = p.parse_args()
a.output.mkdir(exist_ok=True, parents=True)
c = pymysql.connect(host='127.0.0.1', port=a.port, user='root', database='nullable_migration', read_timeout=600)
s = c.cursor()
settings = ['enable_profile=true', 'profile_level=2', 'enable_agg_state=true',
            'parallel_pipeline_task_num=4', 'agg_phase=2', 'enable_sql_cache=false',
            'enable_aggregate_function_null_v2=' + a.v2]
for setting in settings:
    s.execute('set ' + setting)
queries = {}
for nulls in [0, 1, 50, 100]:
    v = f'if(id*17%100<{nulls},null,v)'
    d = f'if(id*17%100<{nulls},null,d)'
    w = f'if(id*13%100<{nulls},null,w)'
    for groups, suffix in [(64, 'low'), (1000000, 'high')]:
        queries[f'numeric_{suffix}_null{nulls}'] = f'''select sum(s*s),sum(a*a),sum(ds*ds),sum(da*da),count(*) from
        (select id%{groups} g,sum({v}) s,avg({v}) a,sum({d}) ds,avg({d}) da from perf_input group by g) t'''
    queries[f'variadic_null{nulls}'] = f'''select sum(a*a) from (select id%100000 g,
      avg_weighted(cast({v} as double),{w}) a from perf_input group by g) t'''
queries['wide_string_set'] = '''select sum(length(m)),sum(n) from
 (select id%64 g,min(if(id%5=0,null,s)) m,array_size(collect_set(if(id%5=0,null,s))) n
  from perf_input group by g) t'''
queries['wide_string_streaming'] = '''select sum(length(m)),sum(n) from
 (select id%1000000 g,min(if(id%5=0,null,s)) m,array_size(collect_set(if(id%5=0,null,s))) n
  from perf_input where id<250000 group by g) t'''
queries['wide_topn'] = '''select sum(length(t)) from
 (select id%64 g,topn(if(id%5=0,null,rpad(s,650,'x')),10) t from perf_input group by g) x'''
queries['stored_merge'] = '''select g,sum_merge(s),avg_merge(a),min_merge(m),avg_weighted_merge(w),
 array_size(collect_set_merge(c)),array_size(array_agg_merge(ar)),map_size(map_agg_v2_merge(mp))
 from old_states group by g order by g'''
queries['stored_union_finalize'] = '''select g,sum_finalize(s),avg_finalize(a),min_finalize(m),avg_weighted_finalize(w)
 from (select g,sum_union(s) s,avg_union(a) a,min_union(m) m,avg_weighted_union(w) w
 from old_states group by g) t order by g'''
queries['stored_bulk_merge'] = """select sum(s0),sum(a0),sum(s1),sum(a1),sum(s50),sum(a50),
 sum(s100),sum(a100),sum(w),count(*) from
 (select g,sum_merge(s0) s0,avg_merge(a0) a0,sum_merge(s1) s1,avg_merge(a1) a1,
 sum_merge(s50) s50,avg_merge(a50) a50,sum_merge(s100) s100,avg_merge(a100) a100,
 avg_weighted_merge(w) w from old_perf_states group by g) t"""
queries['stored_bulk_union_finalize'] = """select sum(sum_finalize(s)),sum(avg_finalize(a)),
 sum(avg_weighted_finalize(w)),count(*) from
 (select g,sum_union(s1) s,avg_union(a1) a,avg_weighted_union(w) w
 from old_perf_states group by g) t"""
queries['stored_bulk_wide'] = """select sum(length(m)),sum(n),count(*) from
 (select g,min_merge(m) m,array_size(collect_set_merge(c)) n from old_perf_states group by g) t"""
root = Path(__file__).resolve().parents[3]
queries['tpcds_sf1_q78'] = (root/'tools/tpcds-tools/queries/sf1/query78.sql').read_text()
records = json.loads((a.output/f'{a.label}-queries.json').read_text())['results'] if a.append else []
opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
for name, sql in queries.items():
    if a.cases and name not in a.cases.split(','):
        continue
    s.execute('use ' + ('nullable_tpcds' if name.startswith('tpcds') else 'nullable_migration'))
    offset = max((r["repetition"]+1 for r in records if r["case"] == name), default=0)
    for iteration in range(-a.warmup_repetitions, a.repetitions):
        rep = iteration+offset if iteration >= 0 else -1
        start = time.perf_counter()
        s.execute(sql)
        result = s.fetchall()
        wall = time.perf_counter() - start
        s.execute('select last_query_id()')
        qid = s.fetchone()[0]
        if rep < 0:
            continue
        url = f'http://127.0.0.1:{a.http_port}/api/profile/text?query_id={qid}'
        req = urllib.request.Request(url, headers={'Authorization': 'Basic cm9vdDo='})
        for retry in range(20):
            profile = opener.open(req, timeout=30).read().decode()
            if 'Profile Completion State: COMPLETE' in profile:
                break
            time.sleep(.1)
        with gzip.open(a.output/f'{a.label}-{name}-{rep}.profile.txt.gz', 'wt') as f:
            f.write(profile)
        records.append({'case': name, 'repetition': rep, 'wall_seconds': wall, 'query_id': qid,
                        'rows': len(result), 'result': [[str(v) if not isinstance(v, (int,float,type(None))) else v for v in row] for row in result], 'result_sha256': hashlib.sha256(repr(result).encode()).hexdigest()})
        print(a.label, name, rep, round(wall, 6), flush=True)
    (a.output/f'{a.label}-queries.json').write_text(json.dumps({'settings': settings, 'warmup_repetitions': a.warmup_repetitions, 'sql': queries,
        'results': records}, indent=2))
