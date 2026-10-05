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
package org.apache.doris.demo.flink.analyze;

import org.apache.doris.demo.flink.User;
import org.apache.doris.flink.cfg.DorisStreamOptions;
import org.apache.doris.flink.datastream.DorisSourceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.Properties;


/**
 * This example mainly demonstrates the use of Flink connector to read Doris data,
 * build DataStream for analysis, and sum.
 * <p>
 * use flink doris connector
 */
public class FlinkDorisConnectorAnalyze {

    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        properties.put("fenodes", "IP:8030");
        properties.put("username", "root");
        properties.put("password", "");
        properties.put("table.identifier", "test1.doris_test_source_2");

        DorisStreamOptions options = new DorisStreamOptions(properties);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource dataStreamSource = env.addSource(new DorisSourceFunction(options, new User()));

        SingleOutputStreamOperator outputStream = dataStreamSource.map(new MapFunction<Object, User>() {
            @Override
            public User map(Object obj) throws Exception {
                User user = new User();
                if (obj instanceof ArrayList<?>) {
                    user.setName(((ArrayList<?>) obj).get(0).toString());
                    user.setAge(Integer.valueOf(((ArrayList<?>) obj).get(1).toString()));
                    user.setPrice(((ArrayList<?>) obj).get(2).toString());
                    user.setSale(((ArrayList<?>) obj).get(3).toString());
                }
                return user;
            }
        });

        outputStream.keyBy(new KeySelector<User, String>() {
            @Override
            public String getKey(User user) throws Exception {
                return user.getName();
            }
        })
                .sum("age")
                .print();

        env.execute("Flink doris connector analyze test");
    }
}
