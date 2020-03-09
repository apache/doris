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

package org.apache.doris.load.loadv2;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalog.Column;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 *  used for build hive global dict and encode source hive table
 *
 *  input: a source hive table
 *  output: a intermediate hive table whose distinct column is encode with int value
 *
 *  usage example
 *  step1,create a intermediate hive table
 *      BuildGlobalDict.createHiveIntermediateTable()
 *  step2, get distinct column's value
 *      BuildGlobalDict.extractDistinctColumn()
 *  step3, build global dict
 *      BuildGlobalDict.buildGlobalDict()
 *  step4, encode intermediate hive table with global dict
 *      BuildGlobalDict.encodeDorisIntermediateHiveTable()
 */

public class BuildGlobalDict {

    protected static final Logger LOG = LoggerFactory.getLogger(BuildGlobalDict.class);

    // name of the column in doris table which need to build global dict
    // currently doris's table column name need to be consistent with the field name in the hive table
    // all column is lowercase
    // TODO(wb): add user customize map from source hive tabe column to doris column
    private List<String> distinctColumnList;
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

    public BuildGlobalDict(List<String> distinctColumnList,
                           List<String> dorisOlapTableColumnList,
                           List<String> mapSideJoinColumns,
                           String sourceHiveDBTableName,
                           String sourceHiveFilter,
                           String dorisHiveDB,
                           String distinctKeyTableName,
                           String globalDictTableName,
                           String dorisIntermediateHiveTable,
                           SparkSession spark) {
        this.distinctColumnList = distinctColumnList;
        this.dorisOlapTableColumnList = dorisOlapTableColumnList;
        this.mapSideJoinColumns = mapSideJoinColumns;
        this.sourceHiveDBTableName = sourceHiveDBTableName;
        this.sourceHiveFilter = sourceHiveFilter;
        this.distinctKeyTableName = distinctKeyTableName;
        this.globalDictTableName = globalDictTableName;
        this.dorisIntermediateHiveTable = dorisIntermediateHiveTable;
        this.spark = spark;

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
        dorisOlapTableColumnList.stream().forEach(columnName -> {
            String columnType = sourceHiveTableColumnInLowercase.get(columnName);
            if (StringUtils.isEmpty(columnType)) {
                throw new RuntimeException(String.format("doris column %s not in source hive table", columnName));
            }
            dorisColumnNameTypeMap.put(columnName, columnType);
        });

        // TODO(wb): drop hive table to prevent schema change
        spark.sql(getCreateIntermediateHiveTableSql());

        spark.sql(getInsertIntermediateHiveTableSql());
    }

    public void extractDistinctColumn() {
        // create distinct tables
        // TODO(wb): maybe keep this table in memory
        spark.sql(getCreateDistinctKeyTableSql());

        // extract distinct column
        for (String column : distinctColumnList) {
            spark.sql(getInsertDistinctKeyTableSql(column, sourceHiveDBTableName));
        }
    }

    // TODO(wb): make build progress concurrently between columns
    // TODO(wb): support 1000 million rows newly added distinct values
    //          spark row_number function support max input about 100 million(more data would cause memoryOverHead,both in-heap and off-heap)
    //          Now I haven't seen such data scale scenario yet.But keep finding better solution is nessassary
    //          such as split data to multiple Dataset and use row_number function to deal separately
    public void buildGlobalDict() {
        // create global dict hive table
        spark.sql(getCreateGlobalDictHiveTableSql());

        for (String distinctColumnName : distinctColumnList) {
            // get global dict max value
            List<Row> maxGlobalDictValueRow = spark.sql(getMaxGlobalDictValueSql(distinctColumnName)).collectAsList();
            if (maxGlobalDictValueRow.size() == 0) {
                throw new RuntimeException(String.format("get max dict value failed: %s", distinctColumnName));
            }
            Object maxDictValueObj = maxGlobalDictValueRow.stream().findAny().get().get(0);
            long maxDictValue = maxDictValueObj == null ? 0 : (long) maxDictValueObj;
            LOG.info(" column {} 's max value in dict is {} ", distinctColumnName, maxDictValue);

            // build global dict
            spark.sql(getCreateBuildGlobalDictSql(maxDictValue, distinctColumnName));
        }
    }

    // encode dorisIntermediateHiveTable's distinct column
    public void encodeDorisIntermediateHiveTable() {
        for (String distinctColumn : distinctColumnList) {
            spark.sql(getEncodeDorisIntermediateHiveTableSql(distinctColumn));
        }
    }

    private String getCreateIntermediateHiveTableSql() {
        StringBuilder sql = new StringBuilder();
        sql.append("create table if not exists " + dorisIntermediateHiveTable + " ( ");

        dorisOlapTableColumnList.stream().forEach(columnName -> {
            sql.append(columnName).append(" ");
            if (distinctColumnList.contains(columnName)) {
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
                + "(dict_key string, dict_value int) partitioned by(dict_column string) stored as sequencefile ";
    }

    private String getMaxGlobalDictValueSql(String distinctColumnName) {
        return "select max(dict_value) from " + globalDictTableName + " where dict_column='" + distinctColumnName + "'";
    }

    private String getCreateBuildGlobalDictSql(long maxGlobalDictValue, String distinctColumnName) {
        return "insert overwrite table " + globalDictTableName + " partition(dict_column='" + distinctColumnName + "') "
                + " select dict_key,dict_value from " + globalDictTableName + " where dict_column='" + distinctColumnName + "' "
                + " union all select t1.dict_key as dict_key,(row_number() over(order by t1.dict_key)) + (" + maxGlobalDictValue + ") as dict_value from "
                + "(select dict_key from " + distinctKeyTableName + " where dict_column='" + distinctColumnName + "' and dict_key is not null)t1 left join " +
                " (select dict_key,dict_value from " + globalDictTableName + " where dict_column='" + distinctColumnName + "' )t2 " +
                "on t1.dict_key = t2.dict_key where t2.dict_value is null";
    }

    private String getEncodeDorisIntermediateHiveTableSql(String distinctColumnName) {
        StringBuilder sql = new StringBuilder();
        sql.append("insert overwrite table ").append(dorisIntermediateHiveTable).append(" select ");
        // using map join to solve distinct column data skew
        // here is a spark sql hint
        if (mapSideJoinColumns.size() != 0 && mapSideJoinColumns.contains(distinctColumnName)) {
            sql.append(" /*+ BROADCAST (t) */ ");
        }
        dorisOlapTableColumnList.forEach(columnName -> {
            if (distinctColumnName.equals(columnName)) {
                sql.append("t.dict_value").append(" ,");
            } else {
                sql.append(dorisIntermediateHiveTable).append(".").append(columnName).append(" ,");
            }
        });
        sql.deleteCharAt(sql.length() - 1)
                .append(" from ")
                .append(dorisIntermediateHiveTable)
                .append(" LEFT OUTER JOIN ( select dict_key,dict_value from ").append(globalDictTableName)
                .append(" where dict_column='").append(distinctColumnName).append("' ) t on ")
                .append(dorisIntermediateHiveTable).append(".").append(distinctColumnName)
                .append(" = t.dict_key ");
        return sql.toString();
    }


}
