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

suite("test_timestampdiff") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_vectorized_engine=true"
    sql "SET enable_fallback_to_original_planner=false" 
    qt_select """SELECT TIMESTAMPDIFF(YEAR,DATE('1981-09-11'),'2022-04-28') AS `date-str`,
                        TIMESTAMPDIFF(YEAR,'1981-09-11','2022-04-28') AS `str-str`,
                        TIMESTAMPDIFF(YEAR,DATE('1981-09-11'),DATE('2022-04-28')) AS `date-date`,
                        TIMESTAMPDIFF(YEAR,'1981-09-11',DATE('2022-04-28')) AS `str-date`"""

    qt_select """SELECT TIMESTAMPDIFF(YEAR,DATE('1981-09-11'),'2022-04-28') AS `date-str`,
                        TIMESTAMPDIFF(YEAR,'1981-09-11','2022-04-28') AS `str-str`,
                        TIMESTAMPDIFF(YEAR,DATE('1981-04-11'),DATE('2022-04-28')) AS `date-date`,
                        TIMESTAMPDIFF(YEAR,'1981-04-11',DATE('2022-04-28')) AS `str-date`"""


    qt_select """SELECT TIMESTAMPDIFF(MONTH,DATE('2020-04-27'),'2022-04-28') AS `date-str`,
                        TIMESTAMPDIFF(MONTH,'2020-04-27','2022-04-28') AS `str-str`,
                        TIMESTAMPDIFF(MONTH,DATE('2020-04-27'),DATE('2022-04-28')) AS `date-date`,
                        TIMESTAMPDIFF(MONTH,'2020-04-27',DATE('2022-04-28')) AS `str-date`"""

    qt_select """SELECT TIMESTAMPDIFF(MONTH,DATE('2020-04-29'),'2022-04-28') AS `date-str`,
                        TIMESTAMPDIFF(MONTH,'2020-04-29','2022-04-28') AS `str-str`,
                        TIMESTAMPDIFF(MONTH,DATE('2020-04-29'),DATE('2022-04-28')) AS `date-date`,
                        TIMESTAMPDIFF(MONTH,'2020-04-29',DATE('2022-04-28')) AS `str-date`"""
}
