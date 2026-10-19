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

"""Keep original operator avg/min/max tokens and join query audit CPU/peak counters."""
import argparse
import gzip
import json
from pathlib import Path
import re
import time

p = argparse.ArgumentParser()
p.add_argument('label')
p.add_argument('audit', type=Path)
p.add_argument('--output', type=Path, default=Path(__file__).parent)
a = p.parse_args()
x = json.loads((a.output/f'{a.label}-queries.json').read_text())
ids = {r['query_id'] for r in x['results']}
# Audit entries are asynchronous; profile completion can precede the audit flush.
for attempt in range(60):
    audit = {}
    raw = []
    for line in a.audit.read_text().splitlines():
        qid = re.search(r'\|QueryId=([^|]+)', line)
        if not qid or qid[1] not in ids:
            continue
        raw.append(line)
        values = dict(re.findall(r'\|([^=|]+)=([^|]*)', line))
        audit[qid[1]] = {key: int(values[key]) for key in ['Time(ms)', 'CpuTimeMS', 'PeakMemoryBytes', 'ScanRows', 'ScanBytes']}
    if ids.issubset(audit):
        break
    time.sleep(1)
records = []
for r in x['results']:
    profile = gzip.open(a.output/f'{a.label}-{r["case"]}-{r["repetition"]}.profile.txt.gz', 'rt').read()
    lines = profile.splitlines()
    operators = []
    for i, line in enumerate(lines):
        if not re.search(r'(?:STREAMING_AGGREGATION|AGGREGATION_SINK)_OPERATOR\(', line):
            continue
        # PlanInfo may precede CommonCounters by many lines. Stay within this
        # operator's indentation boundary instead of assuming fixed offsets.
        indentation = len(line)-len(line.lstrip())
        section = []
        for following in lines[i+1:]:
            if following.strip() and len(following)-len(following.lstrip()) <= indentation:
                break
            section.append(following)
        timers = [v.strip() for v in section if '- ExecTime: avg' in v]
        if timers:
            operators.append({'operator': line.strip(), 'exec_time': timers[0],
                              'memory_peak': next((v.strip() for v in section if '- MemoryUsagePeak:' in v), None)})
    records.append({'case': r['case'], 'repetition': r['repetition'], 'wall_seconds': r['wall_seconds'],
                    'audit': audit.get(r['query_id']), 'operators': operators})
assert all(r['audit'] is not None for r in records), 'Missing audit records; wait for FE audit flush.'
(a.output/f'{a.label}-metrics.json').write_text(json.dumps(records, indent=2))
with gzip.open(a.output/f'{a.label}-audit.log.gz', 'wt') as f:
    f.write('\n'.join(raw)+'\n')
print(a.label, len(records), 'query profiles and audit records joined')
