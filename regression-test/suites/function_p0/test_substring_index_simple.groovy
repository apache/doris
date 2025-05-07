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

suite("test_substring_index_simple") {
    sql "set enable_nereids_planner=true"
    sql "set enable_fallback_to_original_planner=false"

    qt_sql """SELECT substring_index('AAA_01|BBB_02|CCC_03', '|', 1) as result"""

    qt_sql """SELECT substring_index('AAA_01|BBB_02|CCC_03', '|', -1) as result"""

    qt_sql """SELECT substring_index('AAA_01|BBB_02|CCC_03', '|', 2) as result"""

    qt_sql """SELECT substring_index('AAA_01|BBB_02|CCC_03', '|', -2) as result"""

    qt_sql """SELECT substring_index('AAA_01|BBB_02|CCC_03', 'XYZ', 1) as result"""

    qt_sql """SELECT substring_index('AAA_01|BBB_02|CCC_03', 'XYZ', -1) as result"""

    qt_sql """SELECT substring_index('', '|', 1) as result"""

    qt_sql """SELECT substring_index('AAA_01|BBB_02|CCC_03', '', 1) as result"""

    qt_sql """SELECT substring_index('AAA_01|BBB_02|CCC_03', '|', 0) as result"""

    qt_sql """SELECT substring_index('AAA_01|BBB_02|CCC_03', '|', 10) as result"""

    qt_sql """SELECT substring_index('AAA_01|BBB_02|CCC_03', '|', -10) as result"""

    qt_sql """SELECT substring_index('AAA_01||BBB_02||CCC_03', '||', 1) as result"""

    qt_sql """SELECT substring_index('AAA_01||BBB_02||CCC_03', '||', -1) as result"""

    qt_sql """SELECT substring_index('AAA_01|BBB_02|CCC_03', 'BBB', -1) as result"""

    qt_sql """SELECT substring_index('|AAA_01|BBB_02|CCC_03', '|', 1) as result"""

    qt_sql """SELECT substring_index('AAA_01|BBB_02|CCC_03|', '|', -1) as result"""

    qt_sql """SELECT substring_index('北京市|上海市|广州市', '|', 2) as result"""

    qt_sql """SELECT substring_index('北京市分隔符上海市分隔符广州市', '分隔符', 1) as result"""

    qt_sql """SELECT substring_index('北京市分隔符上海市分隔符广州市', '分隔符', -1) as result"""

    qt_sql """SELECT substring_index('hello😀world😀example', '😀', 1) as result"""

    qt_sql """SELECT substring_index('hello😀world😀example', '😀', -1) as result"""

    qt_sql """
    SELECT substring_index('AAA_01|BBB_02|CCC_03', (SELECT '|'), 2) as result
    """

    qt_sql """
    SELECT substring_index('AAA_01|BBB_02|CCC_03', '|', (SELECT 2)) as result
    """

    qt_sql """
    SELECT substring_index('AAA_01|BBB_02|CCC_03', (SELECT '|'), (SELECT 2)) as result
    """

    qt_sql """
    SELECT substring_index('AAA_01|BBB_02|CCC_03', concat('|'), 2) as result
    """

    qt_sql """
    SELECT substring_index('中文_分隔符_测试_分隔符_数据', concat('分', '隔', '符'), 1) as result
    """

    qt_sql """
    SELECT
        substring_index('AAA_01|BBB_02|CCC_03', 'BBB', -1) as result1,
        substring_index('AAA_01|BBB_02|CCC_03', 'bbb', -1) as result2
    """
}