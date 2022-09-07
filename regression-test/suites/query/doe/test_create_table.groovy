import java.util.concurrent.TimeUnit

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

suite("test_create_table", "doe") {
    // create index and put test data
    String es6Test1Command = "curl --head " + context.config.elasticsearch6 + "/test1"
    def es6Test1Process = es6Test1Command.execute()
    es6Test1Process.waitFor()
    def es6Test1Out = es6Test1Process.getText()
    if (es6Test1Out.contains("404")) {
        // if not exist index, create it
        es6Test1Command = "curl -X PUT " + context.config.elasticsearch6 + "/test1 -H Content-Type:application/json -d {\"settings\":{\"index\":{\"number_of_shards\":\"1\",\"number_of_replicas\":\"0\"}},\"mappings\":{\"doc\":{\"properties\":{\"test1\":{\"type\":\"keyword\"},\"test2\":{\"type\":\"double\"}}}}}"
        es6Test1Process = es6Test1Command.execute()
        es6Test1Process.waitFor()
        es6Test1Out = es6Test1Process.getText()
        logger.info("create index command=" + es6Test1Command + " res=" + es6Test1Out)
    }

    sql "CREATE EXTERNAL TABLE IF NOT EXISTS `test_es6` (\n" +
            "  `test1` varchar(100),\n" +
            "  `test2` double\n" +
            ") ENGINE=ELASTICSEARCH\n" +
            "PROPERTIES (\n" +
            "\"hosts\" = \"" + context.config.elasticsearch6 + "\",\n" +
            "\"index\" = \"test1\",\n" +
            "\"type\" = \"doc\",\n" +
            "\"nodes_discovery\" = \"false\"\n" +
            ");"

    qt_sql "select * from test_es6"
}
