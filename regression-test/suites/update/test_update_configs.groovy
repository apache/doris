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

suite("test_update_configs", "p0") {

    String backend_id;
    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);
    backend_id = backendId_to_backendIP.keySet()[0]
    def beIp = backendId_to_backendIP.get(backend_id)
    def bePort = backendId_to_backendHttpPort.get(backend_id)
    def (code, out, err) = show_be_config(beIp, bePort)
    logger.info("Show config: code=" + code + ", out=" + out + ", err=" + err)
    assertEquals(code, 0)
    def configList = parseJson(out.trim())
    assert configList instanceof List

    boolean disableAutoCompaction = true
    boolean enablePrefetch = true
    boolean enableSegcompaction = true
    for (Object ele in (List) configList) {
        assert ele instanceof List<String>
        if (((List<String>) ele)[0] == "disable_auto_compaction") {
            disableAutoCompaction = Boolean.parseBoolean(((List<String>) ele)[2])
        }
        if (((List<String>) ele)[0] == "enable_segcompaction") {
            enableSegcompaction = Boolean.parseBoolean(((List<String>) ele)[2])
        }
    }

    curl("POST", String.format("http://%s:%s/api/update_config?%s=%s&%s=%s", beIp, bePort, "disable_auto_compaction", String.valueOf(!disableAutoCompaction), "enable_segcompaction", String.valueOf(!enableSegcompaction)))


    (code, out, err) = show_be_config(beIp, bePort)
    logger.info("Show config: code=" + code + ", out=" + out + ", err=" + err)
    assertEquals(code, 0)
    def configList2 = parseJson(out.trim())
    assert configList instanceof List
    for (Object ele in (List) configList2) {
        assert ele instanceof List<String>
        if (((List<String>) ele)[0] == "disable_auto_compaction") {
            logger.info("disable_auto_compaction: ${((List<String>) ele)[2]}")
            assertEquals(((List<String>) ele)[2], String.valueOf(!disableAutoCompaction))
        }
        if (((List<String>) ele)[0] == "enable_segcompaction") {
            // enable_segcompaction is not mutable
            logger.info("enable_segcompaction: ${((List<String>) ele)[3]}")
            assertEquals(((List<String>) ele)[2], String.valueOf(enableSegcompaction))
        }
    }

    curl("POST", String.format("http://%s:%s/api/update_config?%s=%s", beIp, bePort, "disable_auto_compaction", String.valueOf(disableAutoCompaction)))

    (code, out, err) = show_be_config(beIp, bePort)
    assertEquals(code, 0)
    configList = parseJson(out.trim())
    assert configList instanceof List
    for (Object ele in (List) configList) {
        assert ele instanceof List<String>
        if (((List<String>) ele)[0] == "disable_auto_compaction") {
            assertEquals(((List<String>) ele)[2], String.valueOf(disableAutoCompaction))
        }
    }
}
