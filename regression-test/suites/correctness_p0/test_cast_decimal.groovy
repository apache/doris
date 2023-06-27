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

suite("test_cast_decimal") {
    sql """
        set enable_nereids_planner=true;
    """

    explain {
        sql """select cast(32123.34212456734 as decimal(3,2));"""
        contains "cast(32123.34212456734 as DECIMALV3(3, 2))"
    }
    

    sql """
        set enable_nereids_planner=false;
    """

    explain {
        sql """select cast(32123.34212456734 as decimal(3,2));"""
        contains "CAST(32123.34212456734 AS DECIMALV3(3, 2))"
    }
}
