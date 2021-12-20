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

package org.apache.doris.load.loadv2.dpp;

import org.apache.commons.collections.map.MultiValueMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalog.Column;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

/**
 *  used for build hive global dict and encode source hive table
 *
 *  input: a source hive table
 *  output: a intermediate hive table whose distinct column is encode with int value
 *
 *  usage example
 *  step1,create a intermediate hive table
 *      GlobalDictBuilder.createHiveIntermediateTable()
 *  step2, get distinct column's value
 *      GlobalDictBuilder.extractDistinctColumn()
 *  step3, build global dict
 *      GlobalDictBuilder.buildGlobalDict()
 *  step4, encode intermediate hive table with global dict
 *      GlobalDictBuilder.encodeDorisIntermediateHiveTable()
 */

public class GlobalDictBuilder {

    protected static final Logger LOG = LoggerFactory.getLogger(GlobalDictBuilder.class);

    // name of the column in doris table which need to build global dict
    // for example: some dict columns a,b,c
    // case 1: all dict columns has no relation, then the map is as below
    //     [a=null, b=null, c=null]
    // case 2: column a's value can reuse column b's value which means column a's value is a subset of column b's value
    //  [b=a,c=null]
    private MultiValueMap dictColumn;
    // target doris table columns in current spark load job
    private List<String> dorisOlapTableColumnList;

    // distinct columns which need to use map join to solve data skew in encodeDorisIntermediateHiveTable()
    // we needn't to specify it until data skew happends
    private List<String> mapSideJoinColumns;

    // hive table datasource,format is db.table
    private String sourceHiveDBTableName;
    // user-specified filter when query sourceHiveDBTable
    private String sourceHiveFilter;
    // intermediate hive table to store the distinct value of distinct column
    private String distinctKeyTableName;
    // current doris table's global dict hive table
    private String globalDictTableName;

    // used for next step to read
    private String dorisIntermediateHiveTable;
    private SparkSession spark;

    // key=doris column name,value=column type
    private Map<String, String> dorisColumnNameTypeMap = new HashMap<>();

    // column in this list means need split distinct value and then encode respectively
    // to avoid the performance bottleneck to transfer origin value to dict value
    private List<String> veryHighCardinalityColumn;
    // determine the split num of new distinct value,better can be divisible by 1
    private int veryHighCardinalityColumnSplitNum;

    private ExecutorService pool;

    private StructType distinctValueSchema;

    public GlobalDictBuilder(MultiValueMap dictColumn,
                             List<String> dorisOlapTableColumnList,
                             List<String> mapSideJoinColumns,
                             String sourceHiveDBTableName,
                             String sourceHiveFilter,
                             String dorisHiveDB,
                             String distinctKeyTableName,
                             String globalDictTableName,
                             String dorisIntermediateHiveTable,
                             int buildConcurrency,
                             List<String> veryHighCardinalityColumn,
                             int veryHighCardinalityColumnSplitNum,
                             SparkSession spark) {
        this.dictColumn = dictColumn;
        this.dorisOlapTableColumnList = dorisOlapTableColumnList;
        this.mapSideJoinColumns = mapSideJoinColumns;
        this.sourceHiveDBTableName = sourceHiveDBTableName;
        this.sourceHiveFilter = sourceHiveFilter;
        this.distinctKeyTableName = distinctKeyTableName;
        this.globalDictTableName = globalDictTableName;
        this.dorisIntermediateHiveTable = dorisIntermediateHiveTable;
        this.spark = spark;
        this.pool = Executors.newFixedThreadPool(buildConcurrency < 0 ? 1 : buildConcurrency);
        this.veryHighCardinalityColumn = veryHighCardinalityColumn;
        this.veryHighCardinalityColumnSplitNum = veryHighCardinalityColumnSplitNum;

        spark.sql("use " + dorisHiveDB);
    }

    public void createHiveIntermediateTable() throws AnalysisException {
        Map<String, String> sourceHiveTableColumn = spark.catalog()
                .listColumns(sourceHiveDBTableName)
                .collectAsList()
                .stream().collect(Collectors.toMap(Column::name, Column::dataType));

        Map<String, String> sourceHiveTableColumnInLowercase = new HashMap<>();
        for (Map.Entry<String, String> entry : sourceHiveTableColumn.entrySet()) {
            sourceHiveTableColumnInLowercase.put(entry.getKey().toLowerCase(), entry.getValue().toLowerCase());
        }

        // check and get doris column type in hive
        dorisOlapTableColumnList.stream().map(String::toLowerCase).forEach(columnName -> {
            String columnType = sourceHiveTableColumnInLowercase.get(columnName);
            if (StringUtils.isEmpty(columnType)) {
                throw new RuntimeException(String.format("doris column %s not in source hive table", columnName));
            }
            dorisColumnNameTypeMap.put(columnName, columnType);
        });

        spark.sql(String.format("drop table if exists %s ", dorisIntermediateHiveTable));
        // create IntermediateHiveTable
        spark.sql(getCreateIntermediateHiveTableSql());

        // insert data to IntermediateHiveTable
        spark.sql(getInsertIntermediateHiveTableSql());
    }

    public void extractDistinctColumn() {
        // create distinct tables
        spark.sql(getCreateDistinctKeyTableSql());

        // extract distinct column
        List<GlobalDictBuildWorker> workerList = new ArrayList<>();
        // For the column in dictColumns's valueSet, their value is a subset of column in keyset,
        // so we don't need to extract distinct value of column in valueSet
        for (Object column : dictColumn.keySet()) {
            workerList.add(()->{
                spark.sql(getInsertDistinctKeyTableSql(column.toString(), dorisIntermediateHiveTable));
            });
        }

        submitWorker(workerList);
    }

    public void buildGlobalDict() throws ExecutionException, InterruptedException {
        // create global dict hive table
        spark.sql(getCreateGlobalDictHiveTableSql());

        List<GlobalDictBuildWorker> globalDictBuildWorkers = new ArrayList<>();
        for (Object distinctColumnNameOrigin : dictColumn.keySet()) {
            String distinctColumnNameTmp = distinctColumnNameOrigin.toString();
            globalDictBuildWorkers.add(()->{
                // get global dict max value
                List<Row> maxGlobalDictValueRow = spark.sql(getMaxGlobalDictValueSql(distinctColumnNameTmp)).collectAsList();
                if (maxGlobalDictValueRow.size() == 0) {
                    throw new RuntimeException(String.format("get max dict value failed: %s", distinctColumnNameTmp));
                }

                long maxDictValue = 0;
                long minDictValue = 0;
                Row row = maxGlobalDictValueRow.get(0);
                if (row != null && row.get(0) != null) {
                    maxDictValue = (long)row.get(0);
                    minDictValue = (long)row.get(1);
                }
                LOG.info(" column " + distinctColumnNameTmp + " 's max value in dict is " + maxDictValue + ", min value is " + minDictValue);
                // maybe never happened, but we need detect it
                if (minDictValue < 0) {
                    throw new RuntimeException(String.format(" column %s 's cardinality has exceed bigint's max value", distinctColumnNameTmp));
                }

                if (veryHighCardinalityColumn.contains(distinctColumnNameTmp) && veryHighCardinalityColumnSplitNum > 1) {
                    // split distinct key first and then encode with count
                    buildGlobalDictBySplit(maxDictValue, distinctColumnNameTmp);
                } else {
                    // build global dict directly
                    spark.sql(getBuildGlobalDictSql(maxDictValue, distinctColumnNameTmp));
                }

            });
        }
        submitWorker(globalDictBuildWorkers);
    }

    // encode dorisIntermediateHiveTable's distinct column
    public void encodeDorisIntermediateHiveTable() {
        for (Object distinctColumnObj : dictColumn.keySet()) {
            spark.sql(getEncodeDorisIntermediateHiveTableSql(distinctColumnObj.toString(), (ArrayList)dictColumn.get(distinctColumnObj.toString())));
        }
    }

    private String getCreateIntermediateHiveTableSql() {
        StringBuilder sql = new StringBuilder();
        sql.append("create table if not exists " + dorisIntermediateHiveTable + " ( ");

        Set<String> allDictColumn = new HashSet<>();
        allDictColumn.addAll(dictColumn.keySet());
        allDictColumn.addAll(dictColumn.values());
        dorisOlapTableColumnList.stream().forEach(columnName -> {
            sql.append(columnName).append(" ");
            if (allDictColumn.contains(columnName)) {
                sql.append(" string ,");
            } else {
                sql.append(dorisColumnNameTypeMap.get(columnName)).append(" ,");
            }
        });
        return sql.deleteCharAt(sql.length() - 1).append(" )").append(" stored as sequencefile ").toString();
    }

    private String getInsertIntermediateHiveTableSql() {
        StringBuilder sql = new StringBuilder();
        sql.append("insert overwrite table ").append(dorisIntermediateHiveTable).append(" select ");
        dorisOlapTableColumnList.stream().forEach(columnName -> {
            sql.append(columnName).append(" ,");
        });
        sql.deleteCharAt(sql.length() - 1)
                .append(" from ").append(sourceHiveDBTableName);
        if (!StringUtils.isEmpty(sourceHiveFilter)) {
            sql.append(" where ").append(sourceHiveFilter);
        }
        return sql.toString();
    }

    private String getCreateDistinctKeyTableSql() {
        return "create table if not exists " + distinctKeyTableName + "(dict_key string) partitioned by (dict_column string) stored as sequencefile ";
    }

    private String getInsertDistinctKeyTableSql(String distinctColumnName, String sourceHiveTable) {
        StringBuilder sql = new StringBuilder();
        sql.append("insert overwrite table ").append(distinctKeyTableName)
                .append(" partition(dict_column='").append(distinctColumnName).append("')")
                .append(" select ").append(distinctColumnName)
                .append(" from ").append(sourceHiveTable)
                .append(" group by ").append(distinctColumnName);
        return sql.toString();
    }

    private String getCreateGlobalDictHiveTableSql() {
        return "create table if not exists " + globalDictTableName
                + "(dict_key string, dict_value bigint) partitioned by(dict_column string) stored as sequencefile ";
    }

    private String getMaxGlobalDictValueSql(String distinctColumnName) {
        return "select max(dict_value) as max_value,min(dict_value) as min_value from " + globalDictTableName + " where dict_column='" + distinctColumnName + "'";
    }

    private void buildGlobalDictBySplit(long maxGlobalDictValue, String distinctColumnName) {
        // 1. get distinct value
        Dataset<Row> newDistinctValue = spark.sql(getNewDistinctValue(distinctColumnName));

        // 2. split the newDistinctValue to avoid window functions' single node bottleneck
        Dataset<Row>[] splitedDistinctValue = newDistinctValue.randomSplit(getRandomSplitWeights());
        long currentMaxDictValue = maxGlobalDictValue;
        Map<String, Long> distinctKeyMap = new HashMap<>();

        for (int i = 0; i < splitedDistinctValue.length; i++) {
            long currentDatasetStartDictValue = currentMaxDictValue;
            long splitDistinctValueCount = splitedDistinctValue[i].count();
            currentMaxDictValue += splitDistinctValueCount;
            String tmpDictTableName = String.format("%s_%s_tmp_dict_%s", i, currentDatasetStartDictValue, distinctColumnName);
            distinctKeyMap.put(tmpDictTableName, currentDatasetStartDictValue);
            Dataset<Row> distinctValueFrame = spark.createDataFrame(splitedDistinctValue[i].toJavaRDD(), getDistinctValueSchema());
            distinctValueFrame.createOrReplaceTempView(tmpDictTableName);
        }

        spark.sql(getSplitBuildGlobalDictSql(distinctKeyMap, distinctColumnName));

    }

    private String getSplitBuildGlobalDictSql(Map<String, Long> distinctKeyMap, String distinctColumnName) {
        StringBuilder sql = new StringBuilder();
        sql.append("insert overwrite table ").append(globalDictTableName).append(" partition(dict_column='").append(distinctColumnName).append("') ")
                .append(" select dict_key,dict_value from ").append(globalDictTableName).append(" where dict_column='").append(distinctColumnName).append("' ");
        for (Map.Entry<String, Long> entry : distinctKeyMap.entrySet()) {
            sql.append(" union all select dict_key, (row_number() over(order by dict_key)) ")
                    .append(String.format(" +(%s) as dict_value from %s", entry.getValue(), entry.getKey()));
        }
        return sql.toString();
    }

    private StructType getDistinctValueSchema() {
        if (distinctValueSchema == null) {
            List<StructField> fieldList = new ArrayList<>();
            fieldList.add(DataTypes.createStructField("dict_key", DataTypes.StringType, false));
            distinctValueSchema = DataTypes.createStructType(fieldList);
        }
        return distinctValueSchema;
    }

    private double[] getRandomSplitWeights() {
        double[] weights = new double[veryHighCardinalityColumnSplitNum];
        double weight = 1 / Double.parseDouble(String.valueOf(veryHighCardinalityColumnSplitNum));
        Arrays.fill(weights, weight);
        return weights;
    }

    private String getBuildGlobalDictSql(long maxGlobalDictValue, String distinctColumnName) {
        return "insert overwrite table " + globalDictTableName + " partition(dict_column='" + distinctColumnName + "') "
                + " select dict_key,dict_value from " + globalDictTableName + " where dict_column='" + distinctColumnName + "' "
                + " union all select t1.dict_key as dict_key,(row_number() over(order by t1.dict_key)) + (" + maxGlobalDictValue + ") as dict_value from "
                + "(select dict_key from " + distinctKeyTableName + " where dict_column='" + distinctColumnName + "' and dict_key is not null)t1 left join "
                + " (select dict_key,dict_value from " + globalDictTableName + " where dict_column='" + distinctColumnName + "' )t2 " +
                "on t1.dict_key = t2.dict_key where t2.dict_value is null";
    }

    private String getNewDistinctValue(String distinctColumnName) {
        return  "select t1.dict_key from " +
                " (select dict_key from " + distinctKeyTableName + " where dict_column='" + distinctColumnName + "' and dict_key is not null)t1 left join " +
                " (select dict_key,dict_value from " + globalDictTableName + " where dict_column='" + distinctColumnName + "' )t2 " +
                "on t1.dict_key = t2.dict_key where t2.dict_value is null";

    }

    private String getEncodeDorisIntermediateHiveTableSql(String dictColumn, List<String> childColumn) {
        StringBuilder sql = new StringBuilder();
        sql.append("insert overwrite table ").append(dorisIntermediateHiveTable).append(" select ");
        // using map join to solve distinct column data skew
        // here is a spark sql hint
        if (mapSideJoinColumns.size() != 0 && mapSideJoinColumns.contains(dictColumn)) {
            sql.append(" /*+ BROADCAST (t) */ ");
        }
        dorisOlapTableColumnList.forEach(columnName -> {
            if (dictColumn.equals(columnName)) {
                sql.append("t.dict_value").append(" ,");
                // means the dictColumn is reused
            } else if (childColumn != null && childColumn.contains(columnName)) {
                sql.append(String.format(" if(%s is null, null, t.dict_value) ", columnName)).append(" ,");
            } else {
                sql.append(dorisIntermediateHiveTable).append(".").append(columnName).append(" ,");
            }
        });
        sql.deleteCharAt(sql.length() - 1)
                .append(" from ")
                .append(dorisIntermediateHiveTable)
                .append(" LEFT OUTER JOIN ( select dict_key,dict_value from ").append(globalDictTableName)
                .append(" where dict_column='").append(dictColumn).append("' ) t on ")
                .append(dorisIntermediateHiveTable).append(".").append(dictColumn)
                .append(" = t.dict_key ");
        return sql.toString();
    }

    private void submitWorker(List<GlobalDictBuildWorker> workerList) {
        try {
            List<Future<Boolean>> futureList = new ArrayList<>();
            for (GlobalDictBuildWorker globalDictBuildWorker : workerList) {
                futureList.add(pool.submit(new Callable<Boolean>() {
                    @Override
                    public Boolean call() throws Exception {
                        try {
                            globalDictBuildWorker.work();
                            return true;
                        } catch (Exception e) {
                            LOG.error("BuildGlobalDict failed", e);
                            return false;
                        }
                    }
                }));
            }

            LOG.info("begin to fetch worker result");
            for (Future<Boolean> future : futureList) {
                if (!future.get()) {
                    throw new RuntimeException("detect one worker failed");
                }
            }
            LOG.info("fetch worker result complete");
        } catch (Exception e) {
            LOG.error("submit worker failed", e);
            throw new RuntimeException("submit worker failed", e);
        }
    }

    private interface GlobalDictBuildWorker {
        void work();
    }
}