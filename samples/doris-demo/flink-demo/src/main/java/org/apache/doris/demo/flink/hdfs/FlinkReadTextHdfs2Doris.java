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
package org.apache.doris.demo.flink.hdfs;


import com.alibaba.fastjson.JSON;
import org.apache.doris.demo.flink.DorisSink;
import org.apache.doris.demo.flink.DorisStreamLoad;
import org.apache.doris.demo.flink.User;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;


/**
 * This example mainly demonstrates how to use flink readTextFile to streamly read hdfs files.
 * And use the doris streamLoad method to write the data into the table specified by doris
 *
 * Support file type:txt,csv
 */
public class FlinkReadTextHdfs2Doris {
    //hdfs path
    private static final String path = "hdfs://IP:8020/tmp/2.csv";
    //hdfs default scheme
    private static final String fsDefaultScheme = "hdfs://IP:8020";
    //doris ip port
    private static final String hostPort = "IP:8030";
    //doris dbName
    private static final String dbName = "test1";
    //doris tbName
    private static final String tbName = "doris_test_source_2";
    //doris userName
    private static final String userName = "root";
    //doris password
    private static final String password = "";
    //doris columns
    private static final String columns = "name,age,price,sale";
    //json format
    private static final String jsonFormat = "[\"$.name\",\"$.age\",\"$.price\",\"$.sale\"]";
    //header
    private final static String header ="name,age,price,sale";


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment blinkStreamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        blinkStreamEnv.enableCheckpointing(10000);
        blinkStreamEnv.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //flink readTextFile
        DataStreamSource<String> dataStreamSource = blinkStreamEnv.readTextFile(path);

        //filter　head
        SingleOutputStreamOperator<String> resultData = dataStreamSource.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                if (header.equals(s)) {
                    System.out.println(s);
                    return false;
                }
                return true;
            }
        }).map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                List<User> users = new ArrayList<>();
                String[] split = s.split("\\,");
                User user = new User();
                user.setName(split[0]);
                user.setAge(Integer.valueOf(split[1]));
                user.setPrice(split[2]);
                user.setSale(split[3]);
                users.add(user);
                return JSON.toJSONString(users);
            }
        });

        DorisStreamLoad dorisStreamLoad = new DorisStreamLoad(hostPort, dbName, tbName, userName, password);

        resultData.addSink(new DorisSink(dorisStreamLoad, columns, jsonFormat));

        resultData.print();

        blinkStreamEnv.execute("flink readText hdfs file to doris");

    }


}
