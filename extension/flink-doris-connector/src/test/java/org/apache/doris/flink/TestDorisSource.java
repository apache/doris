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

package org.apache.doris.flink;

import org.apache.doris.flink.datastream.DorisSource;
import org.apache.doris.flink.deserialization.SimpleListDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.HashMap;
import java.util.Map;



public class TestDorisSource {

    public static void main(String[] args) throws Exception {
        Map<String,String> conf = new HashMap<>();
        conf.put("doris.fenodes","10.220.146.10:8030");
        conf.put("doris.request.auth.user","root");
        conf.put("doris.request.auth.password","");
        conf.put("doris.table.identifier","ods.test_flink_2");
        conf.put("doris.filter.query","rq >= '2019-04-10 00:00:00' and rq <= '2019-05-10 00:00:00'");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.addSource(new DorisSource(conf,new SimpleListDeserializationSchema())).print();

        env.execute("Flink doris test");
    }
}
