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

suite("test_backend_configuration", "p0, external_table,information_schema,backend_configuration") {
    sql("use information_schema")
    qt_sql01 """
        select * from backend_configuration where CONFIGURATION = "brpc_port" and CONFIGURATION_TYPE = "int32_t"
    """

    qt_sql02 """ 
         select * from backend_configuration where CONFIGURATION = "disable_auto_compaction" and CONFIGURATION_TYPE = "bool";
    """

    qt_sql03 """ 
         select * from backend_configuration where CONFIGURATION = "LZ4_HC_compression_level" and CONFIGURATION_TYPE = "int64_t";
    """

}
