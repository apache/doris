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

suite("join_with_column_casesensetive", "arrow_flight_sql") {
    def tables=["ad_order_data_v1","ad_order_data"]

    for (String table in tables) {
        sql """ DROP TABLE IF EXISTS $table """
    }

    for (String table in tables) {
        sql new File("""${context.file.parent}/ddl/${table}.sql""").text
    }

    def result1 = sql """
        explain select ad_order_data.pin_id,  ad_order_data_v1.rptcnt from ad_order_data left join ad_order_data_v1 on ad_order_data.pin_id=ad_order_data_v1.pin_id;
    """

    def result2 = sql """
        explain select ad_order_data.pin_id,  ad_order_data_v1.rptcnt from ad_order_data left join ad_order_data_v1 on ad_order_data.PIN_ID=ad_order_data_v1.PIN_ID;
    """

    assertEquals(result1.toString().toLowerCase(), result2.toString().toLowerCase())
}
