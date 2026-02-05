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
import org.apache.doris.regression.suite.ClusterOptions

suite("test_backend_configuration", "docker, p0, external_table,information_schema,backend_configuration") {
    def options = new ClusterOptions()
    options.setFeNum(1)
    options.setBeNum(3)
    docker(options) {
          // dont select be_id
          def res = sql """ SHOW BACKENDS """

          assertTrue(res.size() == 3)
          
          sql """ 
               select * from information_schema.backend_configuration where CONFIG_NAME = "disable_auto_compaction";
          """
          assertTrue(res.size() == 3)

          res = sql """ 
               select * from information_schema.backend_configuration where CONFIG_NAME = "LZ4_HC_compression_level";
          """
          assertTrue(res.size() == 3)
    }
}
