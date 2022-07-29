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
package org.apache.doris.demo.flink.elasticsearch;


import com.alibaba.fastjson.JSONObject;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.cfg.DorisSink;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.calcite.shaded.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.apache.http.HttpHost;

import java.util.Properties;


/**
 * flink reads elasticsearch data and synchronizes it to doris (flink-doris-connector method)
 */
public class FlinkReadEs2Doris {

    private static final String host = "127.0.0.1";
    private static final String index = "commodity";


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        RowTypeInfo rowTypeInfo = new RowTypeInfo(
                new TypeInformation[]{Types.INT, Types.STRING, Types.STRING,Types.STRING,Types.DOUBLE},
                new String[]{"commodity_id", "commodity_name", "create_time","picture_url","price"});

        ElasticsearchInput es =  ElasticsearchInput.builder(
                        Lists.newArrayList(new HttpHost(host, 9200)),
                        index)
                .setRowTypeInfo(rowTypeInfo)
                .build();

        DataStreamSource<Row> source = env.createInput(es);

        SingleOutputStreamOperator<String> map = source.map(new MapFunction<Row, String>() {
            @Override
            public String map(Row row) throws Exception {
                JSONObject jsonObject = new JSONObject();
                jsonObject.put("commodity_id", row.getField(0));
                jsonObject.put("commodity_name", row.getField(1));
                jsonObject.put("create_time", row.getField(2));
                jsonObject.put("picture_url", row.getField(3));
                jsonObject.put("price", row.getField(4));
                return jsonObject.toJSONString();
            }
        });

        Properties pro = new Properties();
        pro.setProperty("format", "json");
        pro.setProperty("strip_outer_array", "true");

        map.addSink(
                DorisSink.sink(
                        DorisReadOptions.builder().build(),
                        DorisExecutionOptions.builder()
                                .setMaxRetries(3)
                                .setStreamLoadProp(pro).build(),
                        DorisOptions.builder()
                                .setFenodes("127.0.0.1:8030")
                                .setTableIdentifier("test.test_es")
                                .setUsername("root")
                                .setPassword("").build()
                ));

        env.execute("test_es");
    }




}
