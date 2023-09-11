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

package org.apache.doris.load.loadv2.etl;

import org.apache.doris.load.loadv2.dpp.DorisKryoRegistrator;
import org.apache.doris.load.loadv2.dpp.StringAccumulator;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSession.Builder;
import org.apache.spark.util.LongAccumulator;
import org.apache.spark.util.SerializableConfiguration;

import java.io.Serializable;

public class SparkLoadSparkEnv implements Serializable {

    SparkSession spark;

    private LongAccumulator abnormalRowAcc = null;
    private LongAccumulator scannedRowsAcc = null;
    private LongAccumulator fileNumberAcc = null;
    private LongAccumulator fileSizeAcc = null;
    // accumulator to collect invalid rows
    private final StringAccumulator invalidRows = new StringAccumulator();

    // save the hadoop configuration from spark session.
    // because hadoop configuration is not serializable,
    // we need to wrap it so that we can use it in executor.
    private SerializableConfiguration serializableHadoopConf;

    public static SparkLoadSparkEnv build(SparkLoadCommand command) {

        SparkLoadSparkEnv sparkLoadSparkEnv = new SparkLoadSparkEnv();

        sparkLoadSparkEnv.initSparkSession(command);
        sparkLoadSparkEnv.registerAccumulator();

        return sparkLoadSparkEnv;
    }

    public void initSparkSession(SparkLoadCommand command) {

        SparkConf sparkConf = new SparkConf();
        //serialization conf
        sparkConf.set("spark.serializer", KryoSerializer.class.getName());
        sparkConf.set("spark.kryo.registrator", DorisKryoRegistrator.class.getName());
        sparkConf.set("spark.kryo.registrationRequired", "false");

        Builder builder = SparkSession.builder();


        if (command.getEnableHive()) {
            builder.enableHiveSupport();
        }

        spark = builder
                .appName("doris spark load job")
                .config(sparkConf)
                .getOrCreate();

        this.serializableHadoopConf = new SerializableConfiguration(spark.sparkContext().hadoopConfiguration());
    }

    public void registerAccumulator() {
        abnormalRowAcc = spark.sparkContext().longAccumulator("abnormalRowAcc");
        scannedRowsAcc = spark.sparkContext().longAccumulator("scannedRowsAcc");
        fileNumberAcc = spark.sparkContext().longAccumulator("fileNumberAcc");
        fileSizeAcc = spark.sparkContext().longAccumulator("fileSizeAcc");
        spark.sparkContext().register(invalidRows, "InvalidRowsAccumulator");
    }

    public SparkSession getSpark() {
        return spark;
    }

    public Configuration getHadoopConf() {
        return serializableHadoopConf.value();
    }

    public SerializableConfiguration getSerializableConfigurationHadoopConf() {
        return serializableHadoopConf;
    }

    public void addAbnormalRowAcc() {
        abnormalRowAcc.add(1);
    }

    public Long getAbnormalRowAccValue() {
        return abnormalRowAcc.value();
    }

    public void addScannedRowsAcc(Long n) {
        scannedRowsAcc.add(n);
    }

    public Long getScannedRowsAccValue() {
        return scannedRowsAcc.value();
    }

    public void addFileNumberAcc() {
        fileNumberAcc.add(1);
    }

    public Long getFileNumberAccValue() {
        return fileNumberAcc.value();
    }

    public void addFileSizeAcc(long size) {
        fileSizeAcc.add(size);
    }

    public Long getFileSizeAccValue() {
        return fileSizeAcc.value();
    }

    public void addInvalidRows(String row) {
        // at most add 5 rows to invalidRows
        if (abnormalRowAcc.value() < 5) {
            invalidRows.add(row);
        }
    }

    public String getInvalidRowsValue() {
        return invalidRows.value();
    }

    public boolean invalidRowsIsEmpty() {
        return invalidRows.isZero();
    }

    public void stop() {
        spark.stop();
    }
}
