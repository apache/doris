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

suite("test_es_catalog_http_open_api", "p0,external,es,external_docker,external_docker_es") {
    String enabled = context.config.otherConfigs.get("enableEsTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String es_5_port = context.config.otherConfigs.get("es_5_port")
        String es_6_port = context.config.otherConfigs.get("es_6_port")
        String es_7_port = context.config.otherConfigs.get("es_7_port")
        String es_8_port = context.config.otherConfigs.get("es_8_port")

        // test old create-catalog syntax for compatibility
        sql """
            create catalog if not exists test_es_query_es5
            properties (
                "type"="es",
                "elasticsearch.hosts"="http://${externalEnvIp}:$es_5_port",
                "elasticsearch.nodes_discovery"="false",
                "elasticsearch.keyword_sniff"="true"
            );
        """
        sql """
            create catalog if not exists test_es_query_es6
            properties (
                "type"="es",
                "elasticsearch.hosts"="http://${externalEnvIp}:$es_6_port",
                "elasticsearch.nodes_discovery"="false",
                "elasticsearch.keyword_sniff"="true"
            );
        """

        // test new create catalog syntax
        sql """create catalog if not exists test_es_query_es7 properties(
            "type"="es",
            "hosts"="http://${externalEnvIp}:$es_7_port",
            "nodes_discovery"="false",
            "enable_keyword_sniff"="true"
        );
        """

        sql """create catalog if not exists test_es_query_es8 properties(
            "type"="es",
            "hosts"="http://${externalEnvIp}:$es_8_port",
            "nodes_discovery"="false",
            "enable_keyword_sniff"="true"
        );
        """

        List<String> feHosts = getFrontendIpHttpPort()
        // for each catalog 5..8, send a request
        for (int i = 5; i <= 8; i++) {
            String catalog = String.format("test_es_query_es%s", i)
            def (code, out, err) = curl("GET", String.format("http://%s/rest/v2/api/es_catalog/get_mapping?catalog=%s&table=test1", feHosts[0], catalog))
            logger.info("Get mapping response: code=" + code + ", out=" + out + ", err=" + err)
            assertTrue(code == 0)
            assertTrue(out.toLowerCase().contains("success"))
            assertTrue(out.toLowerCase().contains("mappings"))
            assertTrue(out.toLowerCase().contains(catalog))

            String body = '{"query":{"match_all":{}},"stored_fields":"_none_","docvalue_fields":["test6"],"sort":["_doc"],"size":4064}';
            def (code1, out1, err1) = curl("POST", String.format("http://%s/rest/v2/api/es_catalog/search?catalog=%s&table=test1", feHosts[0], catalog), body)
            logger.info("Search index response: code=" + code1 + ", out=" + out1 + ", err=" + err1)
            assertTrue(code1 == 0)
            assertTrue(out1.toLowerCase().contains("success"))
            assertTrue(out1.toLowerCase().contains("hits"))
            assertTrue(out1.toLowerCase().contains(catalog))
        }
    }
}
