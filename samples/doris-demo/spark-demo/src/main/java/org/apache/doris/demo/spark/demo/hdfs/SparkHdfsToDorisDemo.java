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

package org.apache.doris.demo.spark.demo.hdfs;

import lombok.extern.slf4j.Slf4j;
import org.apache.doris.demo.spark.constant.SparkToDorisConstants;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.RDD;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class SparkHdfsToDorisDemo {

    public static void main(String[] args) {

        Map<String, Object> parameters = new HashMap();
        parameters.put("hostPort", SparkToDorisConstants.DORIS_FE_NODES);
        parameters.put("db", SparkToDorisConstants.DORIS_DBNAME);
        parameters.put("tbl", SparkToDorisConstants.DORIS_TABLE_NAME);
        parameters.put("user", SparkToDorisConstants.DORIS_USER);
        parameters.put("password", SparkToDorisConstants.DORIS_PASSWORD);
        SparkConf sparkConf = new SparkConf().setAppName("SparkHdfsToDorisDemo").setMaster("local[1]");
        SparkContext sparkContext = new SparkContext(sparkConf);
        RDD<String> dataRDD = sparkContext.textFile(SparkToDorisConstants.HDFS_PATH, 1);
        dataRDD.foreachPartition(new MyForeachPartitionFunction(parameters));
    }
}

