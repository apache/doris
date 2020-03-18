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

import com.google.gson.FieldNamingPolicy;
import com.google.gson.GsonBuilder;
import org.apache.doris.load.loadv2.etl.EtlJobConfig.EtlColumn;
import org.apache.doris.load.loadv2.etl.EtlJobConfig.EtlFileGroup;
import org.apache.doris.load.loadv2.etl.EtlJobConfig.EtlTable;

import org.apache.spark.sql.SparkSession;

import com.google.common.collect.Lists;
import com.google.gson.Gson;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;

public class SparkEtlJob {
    private static final String BITMAP_TYPE = "bitmap";

    private String configFilePath;
    private EtlJobConfig etlJobConfig;
    private boolean hasBitMapColumns;

    private SparkEtlJob(String configFilePath) {
        this.configFilePath = configFilePath;
    }

    private void initConfig() throws IOException {
        BufferedReader br = null;
        try {
            GsonBuilder gsonBuilder = new GsonBuilder();
            gsonBuilder.setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES);
            Gson gson = gsonBuilder.create();

            br = new BufferedReader(new InputStreamReader(new FileInputStream(new File(configFilePath)),
                                                          "UTF-8"));
            etlJobConfig = gson.fromJson(br.readLine(), EtlJobConfig.class);
            System.err.println("etl job configs: " + etlJobConfig.toString());
        } finally {
            if (br != null) {
                br.close();
            }
        }
    }

    private void checkConfig() throws Exception {
        Map<Long, EtlTable> tables = etlJobConfig.tables;

        // spark etl must have only one table with bitmap type column to process.
        boolean hasBitMapColumns = false;
        for (EtlTable table : tables.values()) {
            for (EtlColumn column : table.columns.values()) {
                if (column.columnType.equalsIgnoreCase(BITMAP_TYPE)) {
                    hasBitMapColumns = true;
                    break;
                }
            }

            if (hasBitMapColumns) {
                break;
            }
        }

        if (hasBitMapColumns && tables.size() != 1) {
            throw new Exception("spark etl job must have only one table with bitmap type column to process");
        }
    }

    private void processDataFromPathes() {
    }

    private void buildGlobalDictAndEncodeSourceTable(EtlTable table, long tableId, SparkSession spark) {
        List<String> distinctColumnList = Lists.newArrayList();
        List<String> dorisOlapTableColumnList = Lists.newArrayList();
        List<String> mapSideJoinColumns = Lists.newArrayList();
        for (EtlColumn column : table.columns.values()) {
            if (column.columnType.equalsIgnoreCase(BITMAP_TYPE)) {
                distinctColumnList.add(column.name);
            }
            dorisOlapTableColumnList.add(column.name);
        }

        EtlFileGroup fileGroup = table.fileGroups.get(0);
        String sourceHiveDBTableName = fileGroup.hiveTableName;
        String dorisHiveDB = sourceHiveDBTableName.split("\\.")[0];
        String sourceHiveFilter = fileGroup.where;

        String taskId = etlJobConfig.outputPath.substring(etlJobConfig.outputPath.lastIndexOf("/") + 1);
        String globalDictTableName = String.format(EtlJobConfig.GLOBAL_DICT_TABLE_NAME, tableId);
        String distinctKeyTableName = String.format(EtlJobConfig.DISTINCT_KEY_TABLE_NAME, tableId, taskId);
        String dorisIntermediateHiveTable = String.format(EtlJobConfig.DORIS_INTERMEDIATE_HIVE_TABLE_NAME,
                                                          tableId, taskId);

        /*
        BuildGlobalDict buildGlobalDict = new BuildGlobalDict(distinctColumnList, dorisOlapTableColumnList,
                                                              mapSideJoinColumns, sourceHiveDBTableName,
                                                              sourceHiveFilter, dorisHiveDB, distinctKeyTableName,
                                                              globalDictTableName, dorisIntermediateHiveTable, spark);
        buildGlobalDict.createHiveIntermediateTable();
        buildGlobalDict.extractDistinctColumn();
        buildGlobalDict.buildGlobalDict();
        buildGlobalDict.encodeDorisIntermediateHiveTable();
         */
    }

    private void processDataFromHiveTable() {
        // only one table
        long tableId = -1;
        EtlTable table = null;
        for (Map.Entry<Long, EtlTable> entry : etlJobConfig.tables.entrySet()) {
            tableId = entry.getKey();
            table = entry.getValue();
            break;
        }

        SparkSession spark = SparkSession.builder().appName("Spark Etl").getOrCreate();

        // build global dict and and encode source hive table
        buildGlobalDictAndEncodeSourceTable(table, tableId, spark);

        // data partition sort and aggregation
    }

    private void processData() {
        if (hasBitMapColumns) {
            processDataFromHiveTable();
        } else {
            processDataFromPathes();
        }
    }

    private void run() throws Exception {
        initConfig();
        checkConfig();
        processData();
    }

    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("missing job config file path");
            System.exit(-1);
        }

        SparkEtlJob etl = new SparkEtlJob(args[0]);
        try {
            etl.run();
        } catch (Exception e) {
            System.err.println("spark etl job run fail");
            e.printStackTrace();
            System.exit(-1);
        }
    }
}
