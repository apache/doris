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

import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.datastream.DorisSourceFunction;
import org.apache.doris.flink.deserialization.SimpleListDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;



public class DorisSourceDataStream {

    public static void main(String[] args) throws Exception {

        DorisOptions.Builder options = DorisOptions.builder()
                .setFenodes("FE_IP:8030")
                .setUsername("root")
                .setPassword("")
                .setTableIdentifier("ods.test_flink_2");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.addSource(new DorisSourceFunction<>(options.build(),new SimpleListDeserializationSchema())).print();
        env.execute("Flink doris test");

//        DataStreamSource<RowData> root = env.createInput(new DorisRowDataInputFormat(options.build())
//                , TypeExtractor.createTypeInfo(RowData.class)
//        );
//        root.print();
//        env.execute("Flink doris test");
    }
}
