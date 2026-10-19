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
| numeric_low_null0 | 39.00 | 32.96 | 33.63 [29.54,43.70] | +2.0% | 249.6 → 213.3 |
| numeric_high_null0 | 175.22 | 168.16 | 146.51 [143.88,148.80] | -12.9% | 765.4 → 734.5 |
| variadic_null0 | 83.98 | 76.24 | 59.53 [55.18,69.16] | -21.9% | 278.8 → 289.5 |
| numeric_low_null1 | 36.30 | 35.03 | 33.23 [31.29,34.49] | -5.2% | 209.4 → 222.6 |
| numeric_high_null1 | 181.52 | 162.80 | 153.60 [150.86,156.19] | -5.7% | 732.8 → 803.9 |
| variadic_null1 | 85.78 | 88.52 | 69.11 [65.49,73.47] | -21.9% | 277.8 → 279.2 |
| numeric_low_null50 | 40.98 | 36.99 | 36.01 [34.56,36.92] | -2.6% | 235.1 → 215.4 |
| numeric_high_null50 | 204.55 | 179.45 | 158.59 [146.55,184.78] | -11.6% | 775.0 → 778.4 |
| variadic_null50 | 86.12 | 83.21 | 60.42 [59.30,61.79] | -27.4% | 256.7 → 288.1 |
| numeric_low_null100 | 32.98 | 30.69 | 29.31 [28.36,31.00] | -4.5% | 220.7 → 203.1 |
| numeric_high_null100 | 105.01 | 153.76 | 106.22 [104.34,109.35] | -30.9% | 769.8 → 781.0 |
| variadic_null100 | 62.79 | 64.70 | 44.80 [41.30,51.20] | -30.7% | 282.6 → 283.7 |
| wide_string_set | 1194.31 | 1184.97 | 1177.90 [1168.34,1201.46] | -0.6% | 2603.1 → 2695.9 |
| wide_string_streaming | 331.16 | 348.54 | 320.40 [304.03,334.42] | -8.1% | 1039.8 → 1035.8 |
| wide_topn | 494.45 | 505.55 | 486.09 [456.67,510.19] | -3.9% | 375.2 → 347.1 |
| stored_merge | 63.76 | 60.86 | 60.17 [52.55,64.70] | -1.1% | 215.1 → 188.1 |
| stored_union_finalize | 15.39 | 14.12 | 13.51 [9.90,18.73] | -4.3% | 218.8 → 157.6 |
| stored_bulk_merge | 42.70 | 39.98 | 42.09 [40.15,44.32] | +5.3% | 244.3 → 245.4 |
| stored_bulk_union_finalize | 27.17 | 25.94 | 25.66 [22.86,27.42] | -1.1% | 172.1 → 163.3 |
| stored_bulk_wide | 723.46 | 722.48 | 709.22 [694.35,747.59] | -1.8% | 1010.9 → 1084.4 |
| tpcds_sf1_q78 | 124.96 | 125.34 | 125.85 [115.74,133.70] | +0.4% | 1678.1 → 1486.5 |
