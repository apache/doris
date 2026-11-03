<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# ORC DATE metadata fixtures

These small, uncompressed files exercise metadata validation independently of
DATE value decoding. They were generated with PyArrow 24.0.0's ORC writer.

- `date_preflight.orc`: schema `struct<id:int,d:date,unused:string>`, two rows
  `(0, DATE ordinal 0, 'a')` and `(0, DATE ordinal 1, 'b')`, one stripe, and
  row-index stride 1000. A predicate `id > 7` excludes the stripe. Projecting
  `d` must not add stripe-footer or row-index reads.
- `date_preflight_bad_index.orc`: identical bytes except that the unused
  column's `ROW_INDEX` stream column ID in the stripe footer is changed from
  3 to 100. The stream itself remains nonempty and valid. No data or index in
  this excluded stripe should be read or parsed during DATE preflight.
- `date_count_truncated_stats.orc`: schema `struct<id:int,s:struct<d:date>>`,
  two rows with IDs 0 and 1 and DATE ordinals 0 and 1. The stripe metadata's
  fourth `colStats` entry (nested DATE ID 3) is removed, while the schema and
  file-level statistics remain complete. Row-index streams are removed, the
  stripe index length and file row-index stride are zero, and footer lengths
  are adjusted. `COUNT(s)` must decline metadata pushdown without an unchecked
  lookup; ordinary reading still returns both intact struct values.
