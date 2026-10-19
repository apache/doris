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
"""Summarize operator task avg/min/max independently of query latency."""
import json
from pathlib import Path
import re
import statistics as st

root = Path(__file__).parent
rows = []
scale = {'ns': 1e-6, 'us': 1e-3, 'ms': 1, 's': 1000}
for label in ['reference-v1','reference-v2','migrated']:
    groups = {}
    for record in json.loads((root/f'{label}-metrics.json').read_text()):
        for ordinal, op in enumerate(record['operators']):
            kind = op['operator'].split('_OPERATOR')[0]
            values = {name: float(number)*scale[unit] for name,number,unit in
                re.findall(r'(avg|max|min) ([0-9.]+)(ns|us|ms|s)',op['exec_time'])}
            assert set(values) == {'avg','min','max'}, op
            key = record['case'],ordinal,kind
            groups.setdefault(key,[]).append(values)
    for (case, ordinal, kind), values in groups.items():
        rows.append({'cohort':label,'case':case,'ordinal':ordinal,'operator':kind,
            'repetitions':len(values),'task_avg_ms_mean':st.mean(v['avg'] for v in values),
            'task_avg_ms_median':st.median(v['avg'] for v in values),
            'task_avg_ms_min':min(v['avg'] for v in values),
            'task_avg_ms_max':max(v['avg'] for v in values),
            'task_min_ms_min':min(v['min'] for v in values),
            'task_max_ms_max':max(v['max'] for v in values)})
(root/'operator-summary.json').write_text(json.dumps(rows,indent=2)+'\n')
lines = ['| Cohort | Case/operator (profile order) | Mean task avg ms | Task min/max across repetitions ms |',
         '| --- | --- | ---: | ---: |']
for r in rows:
    lines.append(f"| {r['cohort']} | {r['case']}/{r['operator']}#{r['ordinal']} | {r['task_avg_ms_mean']:.3f} | {r['task_min_ms_min']:.3f}/{r['task_max_ms_max']:.3f} |")
(root/'operator-summary.md').write_text('<!--\nLicensed to the Apache Software Foundation (ASF) under one\nor more contributor license agreements.  See the NOTICE file\ndistributed with this work for additional information\nregarding copyright ownership.  The ASF licenses this file\nto you under the Apache License, Version 2.0 (the\n"License"); you may not use this file except in compliance\nwith the License.  You may obtain a copy of the License at\n\n  http://www.apache.org/licenses/LICENSE-2.0\n\nUnless required by applicable law or agreed to in writing,\nsoftware distributed under the License is distributed on an\n"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY\nKIND, either express or implied.  See the License for the\nspecific language governing permissions and limitations\nunder the License.\n-->\n\n'+'\n'.join(lines)+'\n')
print('Summarized',len(rows),'operator groups; original avg/min/max retained in metrics JSON.')
