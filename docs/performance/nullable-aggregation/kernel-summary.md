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

| Kernel/rows/NULL%/mode/groups | V1 us | V2 us | Migrated us [min,max] | vs V2 | Heap requests/op V2 → migrated | Heap bytes/op V2 → migrated |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| streaming/nullable_sum/4096/0/1 | 41.157 | 6.403 | 3.244 [3.239,3.250] | -49.3% | 7 → 5 | 139392 → 73856 |
| streaming/nullable_sum/4096/1/1 | 40.880 | 7.162 | 3.258 [3.236,3.299] | -54.5% | 7 → 5 | 139392 → 73856 |
| streaming/nullable_sum/4096/50/1 | 34.872 | 6.468 | 3.243 [3.229,3.260] | -49.9% | 7 → 5 | 139392 → 73856 |
| streaming/nullable_sum/4096/100/1 | 25.848 | 5.158 | 1.812 [1.781,1.822] | -64.9% | 7 → 5 | 139392 → 73856 |
| streaming/nullable_avg/4096/0/1 | 61.733 | 17.663 | 9.192 [9.171,9.236] | -48.0% | 7 → 5 | 434304 → 270464 |
| streaming/nullable_avg/4096/1/1 | 61.264 | 18.108 | 9.269 [9.160,9.624] | -48.8% | 7 → 5 | 434304 → 270464 |
| streaming/nullable_avg/4096/50/1 | 48.052 | 16.661 | 6.448 [6.424,6.516] | -61.3% | 7 → 5 | 434304 → 270464 |
| streaming/nullable_avg/4096/100/1 | 28.086 | 14.055 | 3.456 [3.450,3.467] | -75.4% | 7 → 5 | 434304 → 270464 |
| merge/nullable_sum/4096/0/1/64 | 6.739 | 4.509 | 3.816 [3.556,4.085] | -15.4% | 0.0625 → 0 | 4096 → 0 |
| merge/nullable_sum/4096/0/1/4096 | 13.879 | 11.761 | 10.930 [10.772,11.374] | -7.1% | 0.0625 → 0 | 4096 → 0 |
| merge/nullable_sum/4096/1/1/64 | 6.701 | 4.463 | 3.687 [3.516,3.803] | -17.4% | 0.0625 → 0 | 4096 → 0 |
| merge/nullable_sum/4096/1/1/4096 | 13.926 | 11.777 | 11.816 [11.483,11.964] | +0.3% | 0.0625 → 0 | 4096 → 0 |
| merge/nullable_sum/4096/50/1/64 | 5.760 | 4.527 | 3.097 [2.902,3.261] | -31.6% | 0.0625 → 0 | 4096 → 0 |
| merge/nullable_sum/4096/50/1/4096 | 13.119 | 12.142 | 9.885 [9.470,10.391] | -18.6% | 0.0625 → 0 | 4096 → 0 |
| merge/nullable_sum/4096/100/1/64 | 3.608 | 4.490 | 1.507 [1.472,1.576] | -66.4% | 0.0625 → 0 | 4096 → 0 |
| merge/nullable_sum/4096/100/1/4096 | 10.591 | 12.086 | 8.532 [8.285,8.825] | -29.4% | 0.0625 → 0 | 4096 → 0 |
| merge/nullable_avg/4096/0/1/64 | 12.172 | 7.386 | 7.058 [6.867,7.297] | -4.4% | 0.0625 → 0 | 12288 → 0 |
| merge/nullable_avg/4096/0/1/4096 | 23.171 | 20.204 | 17.947 [17.169,19.542] | -11.2% | 0.0625 → 0 | 12288 → 0 |
| merge/nullable_avg/4096/1/1/64 | 12.257 | 7.634 | 7.497 [6.967,7.784] | -1.8% | 0.0625 → 0 | 12288 → 0 |
| merge/nullable_avg/4096/1/1/4096 | 23.303 | 20.115 | 19.226 [18.555,19.505] | -4.4% | 0.0625 → 0 | 12288 → 0 |
| merge/nullable_avg/4096/50/1/64 | 9.239 | 7.427 | 4.706 [4.489,4.906] | -36.6% | 0.0625 → 0 | 12288 → 0 |
| merge/nullable_avg/4096/50/1/4096 | 19.831 | 20.290 | 16.116 [15.703,16.641] | -20.6% | 0.0625 → 0 | 12288 → 0 |
| merge/nullable_avg/4096/100/1/64 | 6.692 | 7.392 | 1.646 [1.534,1.737] | -77.7% | 0.0625 → 0 | 12288 → 0 |
| merge/nullable_avg/4096/100/1/4096 | 16.726 | 20.013 | 12.149 [11.509,12.782] | -39.3% | 0.0625 → 0 | 12288 → 0 |
| streaming/nullable_decimal_sum/4096/0/1 | 50.397 | 10.170 | 3.056 [2.994,3.138] | -70.0% | 7 → 5 | 237696 → 139392 |
| streaming/nullable_decimal_sum/4096/1/1 | 49.449 | 11.148 | 2.727 [2.680,2.840] | -75.5% | 7 → 5 | 237696 → 139392 |
| streaming/nullable_decimal_sum/4096/50/1 | 40.098 | 9.462 | 2.988 [2.816,3.267] | -68.4% | 7 → 5 | 237696 → 139392 |
| streaming/nullable_decimal_sum/4096/100/1 | 26.379 | 7.625 | 1.975 [1.955,2.011] | -74.1% | 7 → 5 | 237696 → 139392 |
| streaming/nullable_decimal_avg/4096/0/1 | 63.855 | 17.561 | 9.031 [9.026,9.042] | -48.6% | 7 → 5 | 434304 → 270464 |
| streaming/nullable_decimal_avg/4096/1/1 | 63.397 | 18.935 | 8.984 [8.976,8.992] | -52.6% | 7 → 5 | 434304 → 270464 |
| streaming/nullable_decimal_avg/4096/50/1 | 47.169 | 16.865 | 6.263 [6.239,6.291] | -62.9% | 7 → 5 | 434304 → 270464 |
| streaming/nullable_decimal_avg/4096/100/1 | 27.946 | 15.519 | 3.442 [3.430,3.464] | -77.8% | 7 → 5 | 434304 → 270464 |
| merge/nullable_decimal_sum/4096/0/1/64 | 9.662 | 6.105 | 5.648 [5.495,5.794] | -7.5% | 0.0625 → 0 | 8192 → 0 |
| merge/nullable_decimal_sum/4096/0/1/4096 | 17.763 | 14.906 | 13.182 [13.062,13.269] | -11.6% | 0.0625 → 0 | 8192 → 0 |
| merge/nullable_decimal_sum/4096/1/1/64 | 9.185 | 5.905 | 5.982 [5.965,6.017] | +1.3% | 0.0625 → 0 | 8192 → 0 |
| merge/nullable_decimal_sum/4096/1/1/4096 | 17.324 | 14.445 | 13.123 [13.032,13.345] | -9.2% | 0.0625 → 0 | 8192 → 0 |
| merge/nullable_decimal_sum/4096/50/1/64 | 6.806 | 6.064 | 3.575 [3.469,3.732] | -41.0% | 0.0625 → 0 | 8192 → 0 |
| merge/nullable_decimal_sum/4096/50/1/4096 | 14.492 | 14.409 | 10.914 [10.798,11.176] | -24.3% | 0.0625 → 0 | 8192 → 0 |
| merge/nullable_decimal_sum/4096/100/1/64 | 4.725 | 5.953 | 1.496 [1.490,1.510] | -74.9% | 0.0625 → 0 | 8192 → 0 |
| merge/nullable_decimal_sum/4096/100/1/4096 | 11.866 | 14.490 | 8.406 [8.327,8.478] | -42.0% | 0.0625 → 0 | 8192 → 0 |
| merge/nullable_decimal_avg/4096/0/1/64 | 12.874 | 7.530 | 7.286 [7.012,7.635] | -3.2% | 0.0625 → 0 | 12288 → 0 |
| merge/nullable_decimal_avg/4096/0/1/4096 | 23.197 | 20.079 | 17.132 [17.025,17.214] | -14.7% | 0.0625 → 0 | 12288 → 0 |
| merge/nullable_decimal_avg/4096/1/1/64 | 13.129 | 7.403 | 6.976 [6.894,7.052] | -5.8% | 0.0625 → 0 | 12288 → 0 |
| merge/nullable_decimal_avg/4096/1/1/4096 | 23.140 | 20.218 | 17.063 [16.962,17.346] | -15.6% | 0.0625 → 0 | 12288 → 0 |
| merge/nullable_decimal_avg/4096/50/1/64 | 9.710 | 7.502 | 4.264 [4.236,4.294] | -43.2% | 0.0625 → 0 | 12288 → 0 |
| merge/nullable_decimal_avg/4096/50/1/4096 | 19.981 | 20.132 | 15.492 [14.427,21.160] | -23.0% | 0.0625 → 0 | 12288 → 0 |
| merge/nullable_decimal_avg/4096/100/1/64 | 6.799 | 7.525 | 1.709 [1.647,1.781] | -77.3% | 0.0625 → 0 | 12288 → 0 |
| merge/nullable_decimal_avg/4096/100/1/4096 | 16.641 | 20.696 | 12.212 [12.120,12.350] | -41.0% | 0.0625 → 0 | 12288 → 0 |
