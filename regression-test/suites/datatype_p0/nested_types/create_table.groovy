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

suite("create_table_with_nested_type") {
    // register testPlugin function in ${DORIS_HOME}/regression-test/plugins/plugins_create_table_nested_type.groovy
    // input the nested depth and table name, the output is table schema
    // run "bash run-regression-test.sh --run -s create_table_with_nested_type" and watch the logger
    def result = create_table_with_nested_type(1, "test")
    logger.info(result)
}
