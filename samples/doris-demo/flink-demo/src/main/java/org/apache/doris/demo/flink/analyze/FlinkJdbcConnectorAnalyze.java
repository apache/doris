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
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

/**
 * This example mainly demonstrates the use of Flink connector to read Doris data,
 * build DataStream for analysis, and sum.
 * <p>
 * Use custom jdbc
 */

public class FlinkJdbcConnectorAnalyze {
    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        properties.put("url", "jdbc:mysql://IP:9030");
        properties.put("username", "root");
        properties.put("password", "");
        properties.put("sql", "select * from test1.doris_test_source_2");

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);

        DataStreamSource<User> userDataStreamSource = executionEnvironment.addSource(new DorisSource(properties));

        //group
        KeyedStream<User, String> userObjectKeyedStream = userDataStreamSource.keyBy(new KeySelector<User, String>() {
            @Override
            public String getKey(User user) throws Exception {
                return user.getName();
            }
        });
        //reduce
        SingleOutputStreamOperator<User> reduce = userObjectKeyedStream.reduce(new ReduceFunction<User>() {
            @Override
            public User reduce(User t0, User t1) throws Exception {
                int sumAge = t0.getAge() + t1.getAge();
                return new User(t0.getName(), sumAge, t0.getPrice(), t0.getSale());
            }
        });

        reduce.print();

        executionEnvironment.execute("flink custom source analyze test");
    }
}
