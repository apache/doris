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

suite("test_mtmv_ssb_ddl") {

    // ssb_sf1_p1 is writted to test unique key table merge correctly.
    // It creates unique key table and sets bucket num to 1 in order to make sure that
    // many rowsets will be created during loading and then the merge process will be triggered.

    def ssb_tables = ["customer", "lineorder", "part", "date", "supplier"]

    for (String table in ssb_tables) {
        sql new File("""${context.file.parent}/ddl/${table}_create.sql""").text
        sql new File("""${context.file.parent}/ddl/${table}_delete.sql""").text
    }
    
    def ssb_mtmvs = ["ssb_flat", "ssb_q11", "ssb_q12", "ssb_q13", "ssb_q21", "ssb_q22", "ssb_q23",
                     "ssb_q31", "ssb_q32", "ssb_q33", "ssb_q34", "ssb_q31", "ssb_q42", "ssb_q43"]

    for (String mvName in ssb_mtmvs) {
        sql "drop MATERIALIZED VIEW IF EXISTS ${mvName}"
        println "HZW"
        println "run mtmv ddl: ${mvName}"
        sql new File("""${context.file.parent}/mtmv_ddl/${mvName}_create.sql""").text
        waitingMTMVTaskFinished(mvName)
    }
}
