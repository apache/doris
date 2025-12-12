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

suite("test_es_flatten_type", "p0,external,es,external_docker,external_docker_es") {
    String enabled = context.config.otherConfigs.get("enableEsTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String es_7_port = context.config.otherConfigs.get("es_7_port")

        sql """drop catalog if exists test_es7_flatten_type;"""

        sql """create catalog test_es_query_es7_false properties(
            "type"="es",
            "hosts"="http://${externalEnvIp}:$es_7_port",
            "nodes_discovery"="false",
            "enable_keyword_sniff"="true",
            "enable_docvalue_scan" = "false"
        );
        """

        sql """create catalog test_es_query_es7_true properties(
            "type"="es",
            "hosts"="http://${externalEnvIp}:$es_7_port",
            "nodes_discovery"="false",
            "enable_keyword_sniff"="true",
            "enable_docvalue_scan" = "true"
        );
        """

        order_qt_sql1 "select * from test_es_query_es7_false.default_db.test_flatten";
        order_qt_sql2 "select * from test_es_query_es7_true.default_db.test_flatten";
        order_qt_sql3 "select extra from test_es_query_es7_false.default_db.test_flatten";
        order_qt_sql4 "select extra from test_es_query_es7_true.default_db.test_flatten";

    }
}
