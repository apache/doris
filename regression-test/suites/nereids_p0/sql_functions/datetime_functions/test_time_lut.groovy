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

suite("test_time_lut") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    for (def year = 1980; year < 2030; year++) {
        for (def month = 1; month < 13; month++) {
            qt_sql """ select weekofyear('${year}-${month}-1 23:59:59') """
            qt_sql """ select week('${year}-${month}-1 23:59:59') """
            qt_sql """ select yearweek('${year}-${month}-1 23:59:59') """
        }
    }

    for (def year = 1980; year < 2030; year++) {
        for (def month = 1; month < 13; month++) {
            qt_sql """ select week('${year}-${month}-1 23:59:59', 1) """
        }
    }

    for (def year = 1980; year < 2030; year++) {
        for (def month = 1; month < 13; month++) {
            qt_sql """ select week('${year}-${month}-1 23:59:59', 2) """
        }
    }

    for (def year = 1980; year < 2030; year++) {
        for (def month = 1; month < 13; month++) {
            qt_sql """ select week('${year}-${month}-1 23:59:59', 3) """
        }
    }

    for (def year = 1980; year < 2030; year++) {
        for (def month = 1; month < 13; month++) {
            qt_sql """ select week('${year}-${month}-1 23:59:59', 4) """
        }
    }

    for (def year = 1980; year < 2030; year++) {
        for (def month = 1; month < 13; month++) {
            qt_sql """ select week('${year}-${month}-1 23:59:59', 5) """
        }
    }

    for (def year = 1980; year < 2030; year++) {
        for (def month = 1; month < 13; month++) {
            qt_sql """ select week('${year}-${month}-1 23:59:59', 6) """
        }
    }

    for (def year = 1980; year < 2030; year++) {
        for (def month = 1; month < 13; month++) {
            qt_sql """ select week('${year}-${month}-1 23:59:59', 7) """
        }
    }

    for (def year = 1980; year < 2030; year++) {
        for (def month = 1; month < 13; month++) {
            qt_sql """ select yearweek('${year}-${month}-1 23:59:59', 1) """
        }
    }

    for (def year = 1980; year < 2030; year++) {
        for (def month = 1; month < 13; month++) {
            qt_sql """ select yearweek('${year}-${month}-1 23:59:59', 2) """
        }
    }

    for (def year = 1980; year < 2030; year++) {
        for (def month = 1; month < 13; month++) {
            qt_sql """ select yearweek('${year}-${month}-1 23:59:59', 3) """
        }
    }

    for (def year = 1980; year < 2030; year++) {
        for (def month = 1; month < 13; month++) {
            qt_sql """ select yearweek('${year}-${month}-1 23:59:59', 4) """
        }
    }

    for (def year = 1980; year < 2030; year++) {
        for (def month = 1; month < 13; month++) {
            qt_sql """ select yearweek('${year}-${month}-1 23:59:59', 5) """
        }
    }

    for (def year = 1980; year < 2030; year++) {
        for (def month = 1; month < 13; month++) {
            qt_sql """ select yearweek('${year}-${month}-1 23:59:59', 6) """
        }
    }

    for (def year = 1980; year < 2030; year++) {
        for (def month = 1; month < 13; month++) {
            qt_sql """ select yearweek('${year}-${month}-1 23:59:59', 7) """
        }
    }
}
