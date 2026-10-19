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

| Case | V1 ms | V2 ms | Migrated ms [min,max] | vs V2 | Peak MiB (V2 → migrated) |
| --- | ---: | ---: | ---: | ---: | ---: |
| numeric_low_null0 | 39.00 | 32.96 | 32.42 [28.96,37.70] | -1.6% | 249.6 → 234.7 |
| numeric_high_null0 | 175.22 | 168.16 | 170.38 [146.70,201.17] | +1.3% | 765.4 → 785.8 |
| variadic_null0 | 83.98 | 76.24 | 56.02 [53.94,57.93] | -26.5% | 278.8 → 279.1 |
| numeric_low_null1 | 36.30 | 35.03 | 34.47 [30.03,43.13] | -1.6% | 209.4 → 221.7 |
| numeric_high_null1 | 181.52 | 162.80 | 153.39 [150.54,159.88] | -5.8% | 732.8 → 748.9 |
| variadic_null1 | 85.78 | 88.52 | 65.10 [62.27,68.13] | -26.5% | 277.8 → 279.4 |
| numeric_low_null50 | 40.98 | 36.99 | 37.37 [33.49,45.45] | +1.0% | 235.1 → 244.4 |
| numeric_high_null50 | 204.55 | 179.45 | 150.68 [144.53,158.80] | -16.0% | 775.0 → 795.6 |
| variadic_null50 | 86.12 | 83.21 | 63.36 [57.29,80.90] | -23.9% | 256.7 → 277.6 |
| numeric_low_null100 | 32.98 | 30.69 | 29.10 [27.28,30.87] | -5.2% | 220.7 → 179.6 |
| numeric_high_null100 | 105.01 | 153.76 | 112.83 [100.14,131.45] | -26.6% | 769.8 → 813.3 |
| variadic_null100 | 62.79 | 64.70 | 50.20 [44.31,62.83] | -22.4% | 282.6 → 288.5 |
| wide_string_set | 1194.31 | 1184.97 | 1206.30 [1140.44,1245.52] | +1.8% | 2603.1 → 2776.6 |
| wide_string_streaming | 331.16 | 348.54 | 323.95 [306.14,334.87] | -7.1% | 1039.8 → 981.0 |
| wide_topn | 494.45 | 505.55 | 511.14 [453.89,562.09] | +1.1% | 375.2 → 376.2 |
| stored_merge | 63.76 | 60.86 | 56.44 [53.66,59.89] | -7.3% | 215.1 → 203.3 |
| stored_union_finalize | 15.39 | 14.12 | 11.61 [8.93,13.08] | -17.7% | 218.8 → 187.5 |
| stored_bulk_merge | 42.70 | 39.98 | 41.54 [40.44,42.60] | +3.9% | 244.3 → 246.3 |
| stored_bulk_union_finalize | 27.17 | 25.94 | 26.08 [22.38,29.97] | +0.6% | 172.1 → 169.6 |
| stored_bulk_wide | 723.46 | 722.48 | 704.90 [668.02,739.28] | -2.4% | 1010.9 → 988.9 |
| tpcds_sf1_q78 | 124.96 | 125.34 | 118.26 [112.63,122.93] | -5.7% | 1678.1 → 1656.8 |
