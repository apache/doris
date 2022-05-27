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

package org.apache.doris.demo.spark.demo.kafka;


import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;


import org.apache.doris.demo.spark.constant.SparkToDorisConstants;
import org.apache.doris.demo.spark.sink.DorisSink;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;

public class SparkKafkaToDorisDemo {


    public static void main(String[] args) throws TimeoutException, StreamingQueryException {

        SparkConf sparkConf = new SparkConf().setAppName("SparkKafkaToDorisDemo").setMaster("local[1]");
        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
        Dataset<Row> df = sparkSession
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", SparkToDorisConstants.KAFKA_BOOTSTRAP_SERVERS)
                .option("subscribe", SparkToDorisConstants.KAFKA_TOPIC_NAME)
                .option("startingOffsets", SparkToDorisConstants.KAFKA_OFFSET_MODEL)
                .option("max.poll.records", 10)
                .load()
                .selectExpr("CAST(value AS STRING)");

        Map<String, Object> parameters = new HashMap();
        parameters.put("hostPort", SparkToDorisConstants.DORIS_FE_NODES);
        parameters.put("db", SparkToDorisConstants.DORIS_DBNAME);
        parameters.put("tbl", SparkToDorisConstants.DORIS_TABLE_NAME);
        parameters.put("user", SparkToDorisConstants.DORIS_USER);
        parameters.put("password", SparkToDorisConstants.DORIS_PASSWORD);


        df.writeStream().outputMode("append")
                .option("checkpointLocation", SparkToDorisConstants.CHECKPOINT_DIRECTORY)
                .trigger(Trigger.ProcessingTime("20 seconds"))
                .foreach(new DorisSink(parameters))
                .start()
                .awaitTermination();
    }

}
