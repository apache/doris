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
"""Compare kernel CPU time and counted heap requests; keep all repetitions."""
import json
from pathlib import Path
import statistics as st

root = Path(__file__).parent
measurements = {}
for label in ['reference', 'migrated']:
    data = json.loads((root/f'{label}-timing.json').read_text())
    measurements[label] = {}
    for row in data['benchmarks']:
        if row['run_type'] == 'iteration':
            measurements[label].setdefault(row['run_name'], []).append(row['cpu_time'])
allocations = {}
for label in ['reference', 'migrated']:
    allocations[label] = {row['benchmark']: row for row in
        map(json.loads, (root/f'{label}-allocations.jsonl').read_text().splitlines())}
result = []
for name, times in measurements['migrated'].items():
    # The third argument is the historical V1/V2 switch, absent from migrated dispatch.
    args = name.split('/')
    assert args[4] == '1', name
    row = {'case': name}
    for label, mode in [('reference-v1', '0'), ('reference-v2', '1'), ('migrated', '1')]:
        key = '/'.join(args[:4]+[mode]+args[5:])
        source = 'reference' if label.startswith('reference') else label
        values = measurements[source][key]
        heap = allocations[source][key]
        row[label] = {'cpu_ns_mean': st.mean(values), 'cpu_ns_median': st.median(values),
            'cpu_ns_min': min(values), 'cpu_ns_max': max(values),
            'cv': st.stdev(values)/st.mean(values), 'repetitions': len(values), 'last_five_cv': st.stdev(values[-5:])/st.mean(values[-5:]),
            'last_five_cpu_ns_mean': st.mean(values[-5:]),
            'heap_requests_per_operation': heap['requests']/heap['operations'],
            'heap_requested_bytes_per_operation': heap['requested_bytes']/heap['operations']}
    row['change_vs_v2_pct'] = (row['migrated']['cpu_ns_mean']/row['reference-v2']['cpu_ns_mean']-1)*100
    result.append(row)
(root/'kernel-summary.json').write_text(json.dumps(result, indent=2)+'\n')
lines = ['| Kernel/rows/NULL%/mode/groups | V1 us | V2 us | Migrated us [min,max] | vs V2 | Heap requests/op V2 → migrated | Heap bytes/op V2 → migrated |',
    '| --- | ---: | ---: | ---: | ---: | ---: | ---: |']
for r in result:
    a,b,c = [r[k] for k in ['reference-v1','reference-v2','migrated']]
    lines.append(f"| {r['case']} | {a['cpu_ns_mean']/1000:.3f} | {b['cpu_ns_mean']/1000:.3f} | {c['cpu_ns_mean']/1000:.3f} [{c['cpu_ns_min']/1000:.3f},{c['cpu_ns_max']/1000:.3f}] | {r['change_vs_v2_pct']:+.1f}% | {b['heap_requests_per_operation']:g} → {c['heap_requests_per_operation']:g} | {b['heap_requested_bytes_per_operation']:g} → {c['heap_requested_bytes_per_operation']:g} |")
(root/'kernel-summary.md').write_text('<!--\nLicensed to the Apache Software Foundation (ASF) under one\nor more contributor license agreements.  See the NOTICE file\ndistributed with this work for additional information\nregarding copyright ownership.  The ASF licenses this file\nto you under the Apache License, Version 2.0 (the\n"License"); you may not use this file except in compliance\nwith the License.  You may obtain a copy of the License at\n\n  http://www.apache.org/licenses/LICENSE-2.0\n\nUnless required by applicable law or agreed to in writing,\nsoftware distributed under the License is distributed on an\n"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY\nKIND, either express or implied.  See the License for the\nspecific language governing permissions and limitations\nunder the License.\n-->\n\n'+'\n'.join(lines)+'\n')
print('Compared', len(result), 'kernels; native reader scratch is included in reference requests.')
