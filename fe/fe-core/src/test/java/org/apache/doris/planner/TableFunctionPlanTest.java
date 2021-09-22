// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.planner;

public class TableFunctionPlanTest {

    // test planner
    /* Case1 normal table function
      select k1, e1 from table lateral view explode_split(k2, ",") tmp as e1;
     */
    /* Case2 without output explode column
      select k1 from table lateral view explode_split(k2, ",") tmp as e1;
     */
    /* Case3 group by explode column
      select k1, e1, count(*) from table lateral view explode_split(k2, ",") tmp as e1 group by k1 e1;
     */
    /* Case4 where explode column
      select k1, e1 from table lateral view explode_split(k2, ",") tmp as e1 where e1 = "1";
     */
    /* Case5 where normal column
      select k1, e1 from table lateral view explode_split(k2, ",") tmp as e1 where k1 = 1;
     */

    // test explode_split function
    // k1 int ,k2 string
    /* Case1 error param
      select k1, e1 from table lateral view explode_split(k2) tmp as e1;
      select k1, e1 from table lateral view explode_split(k1) tmp as e1;
      select k1, e1 from table lateral view explode_split(k2, k2) tmp as e1;
     */
    /* Case2 table function in where stmt
      select k1 from table where explode_split(k2, ",") = "1";
     */
}
