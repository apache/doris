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
"""Summarize five warmed repetitions and verify result equivalence across cohorts."""
import json
import math
from pathlib import Path
import statistics as st
import argparse

p = argparse.ArgumentParser()
p.add_argument('--input', type=Path, default=Path(__file__).parent)
p.add_argument('--output', type=Path, default=Path(__file__).parent)
p.add_argument('--prefix', default='')
a = p.parse_args()
root = a.input
labels = ['reference-v1', 'reference-v2', 'migrated']
def equivalent(a, b):
    if isinstance(a, list) and isinstance(b, list):
        return len(a) == len(b) and all(equivalent(x, y) for x, y in zip(a, b))
    if isinstance(a, int) and isinstance(b, int):
        return a == b
    if isinstance(a, (int, float)) and isinstance(b, (int, float)):
        return math.isclose(a, b, rel_tol=1e-9, abs_tol=1e-9)
    return a == b
all_queries = {label: json.loads((root / f'{label}-queries.json').read_text()) for label in labels}
all_metrics = {label: json.loads((root / f'{label}-metrics.json').read_text()) for label in labels}
cases = list(all_queries['reference-v1']['sql'])
summary = []
for case in cases:
    row = {'case': case}
    expected = None
    for label in labels:
        records = [r for r in all_queries[label]['results'] if r['case'] == case]
        assert len(records) >= 5, (label, case, len(records))
        metrics = [r for r in all_metrics[label] if r['case'] == case]
        for record in records:
            if expected is None:
                expected = record['result']
            assert equivalent(expected, record['result']), (label, case, expected, record['result'])
        times = [r['wall_seconds'] for r in records]
        row[label] = dict(mean_ms=st.mean(times)*1000, median_ms=st.median(times)*1000,
                          min_ms=min(times)*1000, max_ms=max(times)*1000,
                          cv=st.stdev(times)/st.mean(times),
                          cpu_ms_mean=st.mean(r['audit']['CpuTimeMS'] for r in metrics),
                          cpu_ms_min=min(r['audit']['CpuTimeMS'] for r in metrics),
                          cpu_ms_max=max(r['audit']['CpuTimeMS'] for r in metrics),
                          peak_bytes_mean=st.mean(r['audit']['PeakMemoryBytes'] for r in metrics),
                          peak_bytes_min=min(r['audit']['PeakMemoryBytes'] for r in metrics),
                          peak_bytes_max=max(r['audit']['PeakMemoryBytes'] for r in metrics),
                          scan_rows_per_second=st.median(r['audit']['ScanRows']/r['wall_seconds'] for r in metrics),
                          scan_bytes_per_second=st.median(r['audit']['ScanBytes']/r['wall_seconds'] for r in metrics),
                          source_rows_per_second=4_000_000/st.median(times) if case.startswith(('numeric_', 'variadic_')) else None)
    row['change_vs_v1_pct'] = (row['migrated']['mean_ms']/row['reference-v1']['mean_ms']-1)*100
    row['change_vs_v2_pct'] = (row['migrated']['mean_ms']/row['reference-v2']['mean_ms']-1)*100
    summary.append(row)
(a.output/f'{a.prefix}query-summary.json').write_text(json.dumps(summary, indent=2)+'\n')
lines = ['| Case | V1 ms | V2 ms | Migrated ms [min,max] | vs V2 | Peak MiB (V2 → migrated) |',
         '| --- | ---: | ---: | ---: | ---: | ---: |']
for row in summary:
    x, y, z = (row[label] for label in labels)
    lines.append(f"| {row['case']} | {x['mean_ms']:.2f} | {y['mean_ms']:.2f} | {z['mean_ms']:.2f} [{z['min_ms']:.2f},{z['max_ms']:.2f}] | {row['change_vs_v2_pct']:+.1f}% | {y['peak_bytes_max']/2**20:.1f} → {z['peak_bytes_max']/2**20:.1f} |")
(a.output/f'{a.prefix}query-summary.md').write_text('<!--\nLicensed to the Apache Software Foundation (ASF) under one\nor more contributor license agreements.  See the NOTICE file\ndistributed with this work for additional information\nregarding copyright ownership.  The ASF licenses this file\nto you under the Apache License, Version 2.0 (the\n"License"); you may not use this file except in compliance\nwith the License.  You may obtain a copy of the License at\n\n  http://www.apache.org/licenses/LICENSE-2.0\n\nUnless required by applicable law or agreed to in writing,\nsoftware distributed under the License is distributed on an\n"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY\nKIND, either express or implied.  See the License for the\nspecific language governing permissions and limitations\nunder the License.\n-->\n\n'+'\n'.join(lines)+'\n')
print('Equivalent results:', len(cases), 'cases across all repetitions and cohorts')
print('\n'.join(lines))
