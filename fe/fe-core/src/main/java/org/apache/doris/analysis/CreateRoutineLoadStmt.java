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

package org.apache.doris.analysis;

import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.common.util.Util;
import org.apache.doris.load.RoutineLoadDesc;
import org.apache.doris.load.loadv2.LoadTask;
import org.apache.doris.load.routineload.RoutineLoadJob;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

/*
 Create routine Load statement,  continually load data from a streaming app

 syntax:
      CREATE ROUTINE LOAD [database.]name on table
      [load properties]
      [PROPERTIES
      (
          desired_concurrent_number = xxx,
          max_error_number = xxx,
          k1 = v1,
          ...
          kn = vn
      )]
      FROM type of routine load
      [(
          k1 = v1,
          ...
          kn = vn
      )]

      load properties:
          load property [[,] load property] ...

      load property:
          column separator | columns_mapping | partitions | where

      column separator:
          COLUMNS TERMINATED BY xxx
      columns_mapping:
          COLUMNS (c1, c2, c3 = c1 + c2)
      partitions:
          PARTITIONS (p1, p2, p3)
      where:
          WHERE c1 > 1

      type of routine load:
          KAFKA
*/
public class CreateRoutineLoadStmt extends DdlStmt {
    private static final Logger LOG = LogManager.getLogger(CreateRoutineLoadStmt.class);

    // routine load properties
    public static final String DESIRED_CONCURRENT_NUMBER_PROPERTY = "desired_concurrent_number";
    // max error number in ten thousand records
    public static final String MAX_ERROR_NUMBER_PROPERTY = "max_error_number";
    // the following 3 properties limit the time and batch size of a single routine load task
    public static final String MAX_BATCH_INTERVAL_SEC_PROPERTY = "max_batch_interval";
    public static final String MAX_BATCH_ROWS_PROPERTY = "max_batch_rows";
    public static final String MAX_BATCH_SIZE_PROPERTY = "max_batch_size";
    public static final String EXEC_MEM_LIMIT_PROPERTY = "exec_mem_limit";

    public static final String FORMAT = "format";// the value is csv or json, default is csv
    public static final String STRIP_OUTER_ARRAY = "strip_outer_array";
    public static final String JSONPATHS = "jsonpaths";
    public static final String JSONROOT = "json_root";
    public static final String NUM_AS_STRING = "num_as_string";
    public static final String FUZZY_PARSE = "fuzzy_parse";

    // kafka type properties
    public static final String KAFKA_BROKER_LIST_PROPERTY = "kafka_broker_list";
    public static final String KAFKA_TOPIC_PROPERTY = "kafka_topic";
    // optional
    public static final String KAFKA_PARTITIONS_PROPERTY = "kafka_partitions";
    public static final String KAFKA_OFFSETS_PROPERTY = "kafka_offsets";
    public static final String KAFKA_DEFAULT_OFFSETS = "kafka_default_offsets";
    public static final String KAFKA_ORIGIN_DEFAULT_OFFSETS = "kafka_origin_default_offsets";
    
    private static final String NAME_TYPE = "ROUTINE LOAD NAME";
    public static final String ENDPOINT_REGEX = "[-A-Za-z0-9+&@#/%?=~_|!:,.;]+[-A-Za-z0-9+&@#/%=~_|]";

    private static final ImmutableSet<String> PROPERTIES_SET = new ImmutableSet.Builder<String>()
            .add(DESIRED_CONCURRENT_NUMBER_PROPERTY)
            .add(MAX_ERROR_NUMBER_PROPERTY)
            .add(MAX_BATCH_INTERVAL_SEC_PROPERTY)
            .add(MAX_BATCH_ROWS_PROPERTY)
            .add(MAX_BATCH_SIZE_PROPERTY)
            .add(FORMAT)
            .add(JSONPATHS)
            .add(STRIP_OUTER_ARRAY)
            .add(NUM_AS_STRING)
            .add(FUZZY_PARSE)
            .add(JSONROOT)
            .add(LoadStmt.STRICT_MODE)
            .add(LoadStmt.TIMEZONE)
            .add(EXEC_MEM_LIMIT_PROPERTY)
            .build();

    private final LabelName labelName;
    private final String tableName;
    private final List<ParseNode> loadPropertyList;
    private final Map<String, String> jobProperties;
    private final String typeName;
    private final RoutineLoadDataSourceProperties dataSourceProperties;

    // the following variables will be initialized after analyze
    // -1 as unset, the default value will set in RoutineLoadJob
    private String name;
    private String dbName;
    private RoutineLoadDesc routineLoadDesc;
    private int desiredConcurrentNum = 1;
    private long maxErrorNum = -1;
    private long maxBatchIntervalS = -1;
    private long maxBatchRows = -1;
    private long maxBatchSizeBytes = -1;
    private boolean strictMode = true;
    private long execMemLimit = 2 * 1024 * 1024 * 1024L;
    private String timezone = TimeUtils.DEFAULT_TIME_ZONE;
    /**
     * RoutineLoad support json data.
     * Require Params:
     *   1) dataFormat = "json"
     *   2) jsonPaths = "$.XXX.xxx"
     */
    private String format = ""; //default is csv.
    private String jsonPaths = "";
    private String jsonRoot = ""; // MUST be a jsonpath string
    private boolean stripOuterArray = false;
    private boolean numAsString = false;
    private boolean fuzzyParse = false;

    private LoadTask.MergeType mergeType;

    public static final Predicate<Long> DESIRED_CONCURRENT_NUMBER_PRED = (v) -> v > 0L;
    public static final Predicate<Long> MAX_ERROR_NUMBER_PRED = (v) -> v >= 0L;
    public static final Predicate<Long> MAX_BATCH_INTERVAL_PRED = (v) -> v >= 5 && v <= 60;
    public static final Predicate<Long> MAX_BATCH_ROWS_PRED = (v) -> v >= 200000;
    public static final Predicate<Long> MAX_BATCH_SIZE_PRED = (v) -> v >= 100 * 1024 * 1024 && v <= 1024 * 1024 * 1024;
    public static final Predicate<Long> EXEC_MEM_LIMIT_PRED = (v) -> v >= 0L;

    public CreateRoutineLoadStmt(LabelName labelName, String tableName, List<ParseNode> loadPropertyList,
                                 Map<String, String> jobProperties, String typeName,
                                 Map<String, String> dataSourceProperties, LoadTask.MergeType mergeType) {
        this.labelName = labelName;
        this.tableName = tableName;
        this.loadPropertyList = loadPropertyList;
        this.jobProperties = jobProperties == null ? Maps.newHashMap() : jobProperties;
        this.typeName = typeName.toUpperCase();
        this.dataSourceProperties = new RoutineLoadDataSourceProperties(this.typeName, dataSourceProperties, false);
        this.mergeType = mergeType;
    }

    public String getName() {
        return name;
    }

    public String getDBName() {
        return dbName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getTypeName() {
        return typeName;
    }

    public RoutineLoadDesc getRoutineLoadDesc() {
        return routineLoadDesc;
    }

    public int getDesiredConcurrentNum() {
        return desiredConcurrentNum;
    }

    public long getMaxErrorNum() {
        return maxErrorNum;
    }

    public long getMaxBatchIntervalS() {
        return maxBatchIntervalS;
    }

    public long getMaxBatchRows() {
        return maxBatchRows;
    }

    public long getMaxBatchSize() {
        return maxBatchSizeBytes;
    }

    public long getExecMemLimit() {
        return execMemLimit;
    }

    public boolean isStrictMode() {
        return strictMode;
    }

    public String getTimezone() {
        return timezone;
    }

    public String getFormat() {
        return format;
    }

    public boolean isStripOuterArray() {
        return stripOuterArray;
    }

    public boolean isNumAsString() {
        return numAsString;
    }

    public boolean isFuzzyParse() {
        return fuzzyParse;
    }

    public String getJsonPaths() {
        return jsonPaths;
    }

    public String getJsonRoot() {
        return jsonRoot;
    }

    public String getKafkaBrokerList() {
        return this.dataSourceProperties.getKafkaBrokerList();
    }

    public String getKafkaTopic() {
        return this.dataSourceProperties.getKafkaTopic();
    }

    public List<Pair<Integer, Long>> getKafkaPartitionOffsets() {
        return this.dataSourceProperties.getKafkaPartitionOffsets();
    }

    public Map<String, String> getCustomKafkaProperties() {
        return this.dataSourceProperties.getCustomKafkaProperties();
    }

    public LoadTask.MergeType getMergeType() {
        return mergeType;
    }

    public boolean isOffsetsForTimes() {
        return this.dataSourceProperties.isOffsetsForTimes();
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);
        // check dbName and tableName
        checkDBTable(analyzer);
        // check name
        FeNameFormat.checkCommonName(NAME_TYPE, name);
        // check load properties include column separator etc.
        checkLoadProperties();
        // check routine load job properties include desired concurrent number etc.
        checkJobProperties();
        // check data source properties
        checkDataSourceProperties();
        // analyze merge type
        if (routineLoadDesc != null) {
            routineLoadDesc.analyze(analyzer);
        } else if (mergeType == LoadTask.MergeType.MERGE) {
            throw new AnalysisException("Excepted DELETE ON clause when merge type is MERGE.");
        }
    }

    public void checkDBTable(Analyzer analyzer) throws AnalysisException {
        labelName.analyze(analyzer);
        dbName = labelName.getDbName();
        name = labelName.getLabelName();
        if (Strings.isNullOrEmpty(tableName)) {
            throw new AnalysisException("Table name should not be null");
        }
        Database db = Catalog.getCurrentCatalog().getDb(dbName);
        if (db == null) {
            throw new AnalysisException("database: " + dbName + " not found.");
        }
        Table table = db.getTable(tableName);
        if (table == null) {
            throw new AnalysisException("table: " + tableName + " not found.");
        }
        if (mergeType != LoadTask.MergeType.APPEND
                && (table.getType() != Table.TableType.OLAP
                || ((OlapTable) table).getKeysType() != KeysType.UNIQUE_KEYS)) {
            throw new AnalysisException("load by MERGE or DELETE is only supported in unique tables.");
        }
        if (mergeType != LoadTask.MergeType.APPEND
                && !(table.getType() == Table.TableType.OLAP && ((OlapTable) table).hasDeleteSign()) ) {
            throw new AnalysisException("load by MERGE or DELETE need to upgrade table to support batch delete.");
        }
    }

    public void checkLoadProperties() throws UserException {
        Separator columnSeparator = null;
        // TODO(yangzhengguo01): add line delimiter to properties
        Separator lineDelimiter = null;
        ImportColumnsStmt importColumnsStmt = null;
        ImportWhereStmt precedingImportWhereStmt = null;
        ImportWhereStmt importWhereStmt = null;
        ImportSequenceStmt importSequenceStmt = null;
        PartitionNames partitionNames = null;
        ImportDeleteOnStmt importDeleteOnStmt = null;
        if (loadPropertyList != null) {
            for (ParseNode parseNode : loadPropertyList) {
                if (parseNode instanceof Separator) {
                    // check column separator
                    if (columnSeparator != null) {
                        throw new AnalysisException("repeat setting of column separator");
                    }
                    columnSeparator = (Separator) parseNode;
                    columnSeparator.analyze(null);
                } else if (parseNode instanceof ImportColumnsStmt) {
                    // check columns info
                    if (importColumnsStmt != null) {
                        throw new AnalysisException("repeat setting of columns info");
                    }
                    importColumnsStmt = (ImportColumnsStmt) parseNode;
                } else if (parseNode instanceof ImportWhereStmt) {
                    // check where expr
                    ImportWhereStmt node = (ImportWhereStmt) parseNode;
                    if (node.isPreceding()) {
                        if (precedingImportWhereStmt != null) {
                            throw new AnalysisException("repeat setting of preceding where predicate");
                        }
                        precedingImportWhereStmt = node;
                    } else {
                        if (importWhereStmt != null) {
                            throw new AnalysisException("repeat setting of where predicate");
                        }
                        importWhereStmt = node;
                    }
                } else if (parseNode instanceof PartitionNames) {
                    // check partition names
                    if (partitionNames != null) {
                        throw new AnalysisException("repeat setting of partition names");
                    }
                    partitionNames = (PartitionNames) parseNode;
                    partitionNames.analyze(null);
                } else if (parseNode instanceof ImportDeleteOnStmt) {
                    // check delete expr
                    if (importDeleteOnStmt != null) {
                        throw new AnalysisException("repeat setting of delete predicate");
                    }
                    importDeleteOnStmt = (ImportDeleteOnStmt) parseNode;
                } else if (parseNode instanceof ImportSequenceStmt) {
                    // check sequence column
                    if (importSequenceStmt != null) {
                        throw new AnalysisException("repeat setting of sequence column");
                    }
                    importSequenceStmt = (ImportSequenceStmt) parseNode;
                }
            }
        }
        routineLoadDesc = new RoutineLoadDesc(columnSeparator, lineDelimiter, importColumnsStmt,
                        precedingImportWhereStmt, importWhereStmt,
                        partitionNames, importDeleteOnStmt == null ? null : importDeleteOnStmt.getExpr(), mergeType,
                        importSequenceStmt == null ? null : importSequenceStmt.getSequenceColName());
    }

    private void checkJobProperties() throws UserException {
        Optional<String> optional = jobProperties.keySet().stream().filter(
                entity -> !PROPERTIES_SET.contains(entity)).findFirst();
        if (optional.isPresent()) {
            throw new AnalysisException(optional.get() + " is invalid property");
        }

        desiredConcurrentNum = ((Long) Util.getLongPropertyOrDefault(jobProperties.get(DESIRED_CONCURRENT_NUMBER_PROPERTY),
                Config.max_routine_load_task_concurrent_num, DESIRED_CONCURRENT_NUMBER_PRED,
                DESIRED_CONCURRENT_NUMBER_PROPERTY + " should > 0")).intValue();
        
        maxErrorNum = Util.getLongPropertyOrDefault(jobProperties.get(MAX_ERROR_NUMBER_PROPERTY),
                RoutineLoadJob.DEFAULT_MAX_ERROR_NUM, MAX_ERROR_NUMBER_PRED,
                MAX_ERROR_NUMBER_PROPERTY + " should >= 0");
        
        maxBatchIntervalS = Util.getLongPropertyOrDefault(jobProperties.get(MAX_BATCH_INTERVAL_SEC_PROPERTY),
                RoutineLoadJob.DEFAULT_MAX_INTERVAL_SECOND, MAX_BATCH_INTERVAL_PRED,
                MAX_BATCH_INTERVAL_SEC_PROPERTY + " should between 5 and 60");
        
        maxBatchRows = Util.getLongPropertyOrDefault(jobProperties.get(MAX_BATCH_ROWS_PROPERTY),
                RoutineLoadJob.DEFAULT_MAX_BATCH_ROWS, MAX_BATCH_ROWS_PRED,
                MAX_BATCH_ROWS_PROPERTY + " should > 200000");
        
        maxBatchSizeBytes = Util.getLongPropertyOrDefault(jobProperties.get(MAX_BATCH_SIZE_PROPERTY),
                RoutineLoadJob.DEFAULT_MAX_BATCH_SIZE, MAX_BATCH_SIZE_PRED,
                MAX_BATCH_SIZE_PROPERTY + " should between 100MB and 1GB");

        strictMode = Util.getBooleanPropertyOrDefault(jobProperties.get(LoadStmt.STRICT_MODE),
                                                      RoutineLoadJob.DEFAULT_STRICT_MODE,
                                                      LoadStmt.STRICT_MODE + " should be a boolean");
        execMemLimit = Util.getLongPropertyOrDefault(jobProperties.get(EXEC_MEM_LIMIT_PROPERTY),
                RoutineLoadJob.DEFAULT_EXEC_MEM_LIMIT, EXEC_MEM_LIMIT_PRED, EXEC_MEM_LIMIT_PROPERTY + "should > 0");
        if (ConnectContext.get() != null) {
            timezone = ConnectContext.get().getSessionVariable().getTimeZone();
        }
        timezone = TimeUtils.checkTimeZoneValidAndStandardize(jobProperties.getOrDefault(LoadStmt.TIMEZONE, timezone));

        format = jobProperties.get(FORMAT);
        if (format != null) {
            if (format.equalsIgnoreCase("csv")) {
                format = ""; // if it's not json, then it's mean csv and set empty
            } else if (format.equalsIgnoreCase("json")) {
                format = "json";
                jsonPaths = jobProperties.get(JSONPATHS);
                jsonRoot = jobProperties.get(JSONROOT);
                stripOuterArray = Boolean.valueOf(jobProperties.getOrDefault(STRIP_OUTER_ARRAY, "false"));
                numAsString = Boolean.valueOf(jobProperties.getOrDefault(NUM_AS_STRING, "false"));
                fuzzyParse = Boolean.valueOf(jobProperties.getOrDefault(FUZZY_PARSE, "false"));
            } else {
                throw new UserException("Format type is invalid. format=`" + format + "`");
            }
        } else {
            format = "csv"; // default csv
        }
    }

    private void checkDataSourceProperties() throws UserException {
        this.dataSourceProperties.setTimezone(this.timezone);
        this.dataSourceProperties.analyze();
    }
}
