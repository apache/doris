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

package org.apache.doris.nereids.minidump;

import org.apache.doris.catalog.ColocateTableIndex;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.SchemaTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.View;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.proc.FrontendsProcNode;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.AbstractPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.VariableMgr;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.ColumnStatisticBuilder;
import org.apache.doris.statistics.Histogram;

import com.google.common.collect.Maps;
import com.google.gson.reflect.TypeToken;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Util for minidump
 */
public class MinidumpUtils {

    private static final Logger LOG = LogManager.getLogger(MinidumpUtils.class);

    private static String DUMP_PATH = Config.spilled_minidump_storage_path;

    private static String DUMP_FILE_FULL_PATH = null;

    private static String HTTP_GET_STRING = null;

    private static boolean dump = false;

    private static final int FE_VERSION_PREFIX_LENGTH = 13;

    public static void openDump() {
        dump = true;
    }

    public static boolean isDump() {
        return dump;
    }

    public static String getDumpFileFullPath() {
        return DUMP_FILE_FULL_PATH;
    }

    public static String getHttpGetString() {
        return HTTP_GET_STRING;
    }

    /**
     * Saving of minidump file to fe log path
     */
    public static void saveMinidumpString(JSONObject minidump, String querId) {
        String dumpPath = MinidumpUtils.DUMP_PATH + File.separator + "_" + querId;
        String feAddress = FrontendsProcNode.getCurrentFrontendVersion(Env.getCurrentEnv()).getHost();
        int feHttpPort = Config.http_port;
        MinidumpUtils.DUMP_FILE_FULL_PATH = dumpPath + ".json";
        MinidumpUtils.HTTP_GET_STRING = "http://" + feAddress + ":" + feHttpPort + "/api/minidump?query_id=" + querId;
        String jsonMinidump = minidump.toString(4);
        try (FileWriter file = new FileWriter(MinidumpUtils.DUMP_FILE_FULL_PATH)) {
            file.write(jsonMinidump);
        } catch (IOException e) {
            LOG.info("failed to save minidump file", e);
        }
    }

    /**
     * deserialize of tables using gson, we need to get table type first
     */
    private static Map<List<String>, TableIf> dserializeTables(JSONArray tablesJson) {
        Map<List<String>, TableIf> tables = Maps.newHashMap();
        for (int i = 0; i < tablesJson.length(); i++) {
            JSONObject tableJson = (JSONObject) tablesJson.get(i);
            TableIf newTable;
            switch ((String) tableJson.get("TableType")) {
                case "OLAP":
                    String tableJsonValue = tableJson.get("TableValue").toString();
                    newTable = GsonUtils.GSON.fromJson(tableJsonValue, OlapTable.class);
                    break;
                case "VIEW":
                    String viewJsonValue = tableJson.get("TableValue").toString();
                    newTable = GsonUtils.GSON.fromJson(viewJsonValue, View.class);
                    ((View) newTable).setInlineViewDefWithSqlMode(tableJson.get("InlineViewDef").toString(),
                            tableJson.getLong("SqlMode"));
                    break;
                default:
                    newTable = null;
                    break;
            }
            Type listType = new TypeToken<List<String>>() {}.getType();
            List<String> key = GsonUtils.GSON.fromJson(tableJson.getString("TableName"), listType);
            tables.put(key, newTable);
        }
        return tables;
    }

    /**
     * Load minidump to memory using string
     */
    public static Minidump jsonMinidumpLoadFromString(String inputString) throws Exception {
        // Parse the JSON string back into a JSON object
        JSONObject inputJSON = new JSONObject(inputString);
        String dumpFeVersion = inputJSON.getString("FeVersion");
        String currFeVersion = FrontendsProcNode.getCurrentFrontendVersion(Env.getCurrentEnv()).getVersion()
                .substring(FE_VERSION_PREFIX_LENGTH);
        if (!currFeVersion.equals(dumpFeVersion)) {
            throw new AnalysisException("fe version:" + currFeVersion
                    + " does not match dump fe version: " + dumpFeVersion);
        }
        SessionVariable newSessionVariable = new SessionVariable();
        newSessionVariable.readFromJson(inputJSON.getString("SessionVariable"));
        String sql = inputJSON.getString("Sql");

        JSONArray tablesJson = (JSONArray) inputJSON.get("Tables");
        Map<List<String>, TableIf> tables = dserializeTables(tablesJson);

        String colocateTableIndexJson = inputJSON.get("ColocateTableIndex").toString();
        ColocateTableIndex newColocateTableIndex
                = GsonUtils.GSON.fromJson(colocateTableIndexJson, ColocateTableIndex.class);

        JSONArray columnStats = (JSONArray) inputJSON.get("ColumnStatistics");
        Map<String, ColumnStatistic> columnStatisticMap = new HashMap<>();
        for (int i = 0; i < columnStats.length(); i++) {
            JSONObject oneColumnStat = (JSONObject) columnStats.get(i);
            String colName = oneColumnStat.keys().next();
            String colStat = oneColumnStat.getString(colName);
            ColumnStatistic columnStatistic = GsonUtils.GSON.fromJson(colStat, ColumnStatistic.class);
            columnStatisticMap.put(colName, columnStatistic);
        }
        JSONArray histogramJsonArray = (JSONArray) inputJSON.get("Histogram");
        Map<String, Histogram> histogramMap = new HashMap<>();
        for (int i = 0; i < histogramJsonArray.length(); i++) {
            JSONObject histogramJson = (JSONObject) histogramJsonArray.get(i);
            String colName = histogramJson.keys().next();
            String colHistogram = histogramJson.getString(colName);
            Histogram histogram = Histogram.deserializeFromJson(colHistogram);
            histogramMap.put(colName, histogram);
        }
        String parsedPlanJson = inputJSON.getString("ParsedPlan");
        String resultPlanJson = inputJSON.getString("ResultPlan");
        String dbName = inputJSON.getString("DbName");

        return new Minidump(sql, newSessionVariable, parsedPlanJson, resultPlanJson,
            tables, dbName, columnStatisticMap, histogramMap, newColocateTableIndex);
    }

    /**
     * Loading of minidump file
     */
    public static Minidump jsonMinidumpLoad(String dumpFilePath) throws Exception {
        // open file, read file, put them into minidump object
        try (FileInputStream inputStream = new FileInputStream(dumpFilePath)) {
            StringBuilder sb = new StringBuilder();
            int ch;
            while ((ch = inputStream.read()) != -1) {
                sb.append((char) ch);
            }
            String inputString = sb.toString();
            return jsonMinidumpLoadFromString(inputString);
        } catch (IOException e) {
            LOG.info("failed to open minidump file", e);
        }
        return null;
    }

    /**
     * Setting connectContext using minidump parameters
     * @param minidump minidump in memory
     */
    public static void setConnectContext(Minidump minidump) {
        ConnectContext connectContext = new ConnectContext();
        connectContext.getTotalColumnStatisticMap().putAll(minidump.getTotalColumnStatisticMap());
        connectContext.getTotalHistogramMap().putAll(minidump.getTotalHistogramMap());
        connectContext.setThreadLocalInfo();
        Env.getCurrentEnv().setColocateTableIndex(minidump.getColocateTableIndex());
        connectContext.setSessionVariable(minidump.getSessionVariable());
        connectContext.setTables(minidump.getTables());
        connectContext.setDatabase(minidump.getDbName());
        connectContext.getSessionVariable().setPlanNereidsDump(true);
        connectContext.getSessionVariable().enableNereidsTimeout = false;
    }

    /**
     * Loading minidump messages from file to memory
     * @param minidumpPath path of minidump file
     * @return minidump messages in memory
     */
    public static Minidump loadMinidumpInputs(String minidumpPath) throws AnalysisException {
        Minidump minidump = null;
        try {
            minidump = MinidumpUtils.jsonMinidumpLoad(minidumpPath);
        } catch (AnalysisException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        setConnectContext(minidump);

        Env env = Env.getCurrentEnv();
        ConnectContext.get().setEnv(env);
        ConnectContext.get().getSessionVariable().setEnableNereidsTrace(false);
        ConnectContext.get().getSessionVariable().setNereidsTraceEventMode("all");
        return minidump;
    }

    /**
     * Executing sql with minidump and return jsontype result for more use of unit test
     * @param sql original sql clause
     * @return JSONObject of result plan
     */
    public static JSONObject executeSql(String sql) {
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan parsed = nereidsParser.parseSingle(sql);
        if (parsed instanceof ExplainCommand) {
            parsed = ((ExplainCommand) parsed).getLogicalPlan();
        }
        NereidsPlanner nereidsPlanner = new NereidsPlanner(
                new StatementContext(ConnectContext.get(), new OriginStatement(sql, 0)));
        nereidsPlanner.planWithLock(LogicalPlanAdapter.of(parsed));
        return ((AbstractPlan) nereidsPlanner.getOptimizedPlan()).toJson();
    }

    /**
     * serialize tables from Table in catalog to json format
     */
    public static JSONArray serializeTables(Map<List<String>, TableIf> tables) {
        JSONArray tablesJson = new JSONArray();
        for (Map.Entry<List<String>, TableIf> table : tables.entrySet()) {
            String tableValues = GsonUtils.GSON.toJson(table.getValue());
            JSONObject oneTableJson = new JSONObject();
            oneTableJson.put("TableType", table.getValue().getType());
            oneTableJson.put("TableName", GsonUtils.GSON.toJson(table.getKey()));
            if (table.getValue() instanceof View) {
                oneTableJson.put("InlineViewDef", ((View) table.getValue()).getInlineViewDef());
                oneTableJson.put("SqlMode", Long.toString(((View) table.getValue()).getSqlMode()));
            }
            JSONObject jsonTableValues = new JSONObject(tableValues);
            oneTableJson.put("TableValue", jsonTableValues);
            tablesJson.put(oneTableJson);
        }
        return tablesJson;
    }

    private static JSONObject serializeColocateTableIndex(ColocateTableIndex colocateTableIndex) {
        String colocatedTableIndexJson = GsonUtils.GSON.toJson(colocateTableIndex);
        return new JSONObject(colocatedTableIndexJson);
    }

    private static ColumnStatistic getColumnStatistic(TableIf table, String colName) {
        // TODO. Get index id for materialized view.
        return Env.getCurrentEnv().getStatisticsCache().getColumnStatistics(
            table.getDatabase().getCatalog().getId(), table.getDatabase().getId(), table.getId(), -1, colName);
    }

    private static Histogram getColumnHistogram(TableIf table, String colName) {
        return Env.getCurrentEnv().getStatisticsCache().getHistogram(
                table.getDatabase().getCatalog().getId(), table.getDatabase().getId(), table.getId(), colName);
    }

    /**
     * serialize column statistic and histograms when loading to dumpfile and environment
     */
    private static void serializeStatsUsed(JSONObject jsonObj, Map<List<String>, TableIf> tables) {
        JSONArray columnStatistics = new JSONArray();
        JSONArray histograms = new JSONArray();
        for (Map.Entry<List<String>, TableIf> tableEntry : tables.entrySet()) {
            TableIf table = tableEntry.getValue();
            if (table instanceof SchemaTable) {
                continue;
            }
            List<Column> columns = table.getColumns();
            for (Column column : columns) {
                String colName = column.getName();
                ColumnStatistic cache =
                        ConnectContext.get().getSessionVariable().enableStats
                        ? getColumnStatistic(table, colName) : ColumnStatistic.UNKNOWN;
                if (cache.avgSizeByte <= 0) {
                    cache = new ColumnStatisticBuilder(cache)
                        .setAvgSizeByte(column.getType().getSlotSize())
                        .build();
                }

                Histogram histogram = getColumnHistogram(table, colName);
                if (histogram != null) {
                    JSONObject oneHistogram = new JSONObject();
                    oneHistogram.put(table.getName() + colName, Histogram.serializeToJson(histogram));
                    histograms.put(oneHistogram);
                }
                JSONObject oneColumnStats = new JSONObject();
                oneColumnStats.put(table.getName() + colName, GsonUtils.GSON.toJson(cache));
                columnStatistics.put(oneColumnStats);
            }
        }
        jsonObj.put("ColumnStatistics", columnStatistics);
        jsonObj.put("Histogram", histograms);
    }

    /**
     * serialize output plan to dump file and persistent into disk
     */
    public static void serializeOutputToDumpFile(Plan resultPlan) {
        ConnectContext connectContext = ConnectContext.get();
        if (!isDump()) {
            return;
        }
        connectContext.getMinidump().put("ResultPlan", ((AbstractPlan) resultPlan).toJson());
        if (isDump()) {
            saveMinidumpString(connectContext.getMinidump(), DebugUtil.printId(connectContext.queryId()));
        }
    }

    /** compare two json object and print detail information about difference */
    public static List<String> compareJsonObjects(JSONObject json1, JSONObject json2, String path) {
        List<String> differences = new ArrayList<>();

        Iterator<String> keys = json1.keys();
        while (keys.hasNext()) {
            String key = keys.next();
            String currentPath = "";
            if (path.isEmpty()) {
                if (key.equals("PlanType")) {
                    path = json1.getString(key);
                } else {
                    currentPath = key;
                }
            } else {
                if (key.equals("PlanType")) {
                    currentPath = path + "." + json1.getString(key);
                } else {
                    currentPath = path + "." + key;
                }
            }

            if (!json2.has(key)) {
                differences.add("Key '" + currentPath + "' not found in the second JSON object.");
                continue;
            }

            Object value1 = json1.get(key);
            Object value2 = json2.get(key);

            if (!value1.toString().equals(value2.toString())) {
                differences.add("Value for key '" + currentPath + "' is different: " + value1 + " != " + value2);
            }

            if (value1 instanceof JSONObject && value2 instanceof JSONObject) {
                List<String> nestedDifferences =
                        compareJsonObjects((JSONObject) value1, (JSONObject) value2, currentPath);
                differences.addAll(nestedDifferences);
            } else if (value1 instanceof JSONArray && value2 instanceof JSONArray) {
                List<String> nestedDifferences = compareJsonArrays((JSONArray) value1, (JSONArray) value2, currentPath);
                differences.addAll(nestedDifferences);
            }
        }

        keys = json2.keys();
        while (keys.hasNext()) {
            String key = keys.next();
            String currentPath = (path.isEmpty()) ? key : path + "." + key;

            if (!json1.has(key)) {
                differences.add("Key '" + currentPath + "' not found in the first JSON object.");
            }
        }

        return differences;
    }

    private static List<String> compareJsonArrays(JSONArray array1, JSONArray array2, String path) {
        List<String> differences = new ArrayList<>();

        if (array1.length() != array2.length()) {
            differences.add("Array length for key '" + path + "' is different: "
                    + array1.length() + " != " + array2.length());
            return differences;
        }

        for (int i = 0; i < array1.length(); i++) {
            Object value1 = array1.get(i);
            Object value2 = array2.get(i);
            String currentPath = path + "[" + i + "]";

            if (value1 instanceof JSONObject && value2 instanceof JSONObject) {
                List<String> nestedDifferences =
                        compareJsonObjects((JSONObject) value1, (JSONObject) value2, currentPath);
                differences.addAll(nestedDifferences);
            } else if (!value1.equals(value2)) {
                differences.add("Value for key '" + currentPath + "' is different: " + value1 + " != " + value2);
            }
        }

        return differences;
    }

    private static String getValue(Object obj, Field field) {
        try {
            switch (field.getType().getSimpleName()) {
                case "boolean":
                    return Boolean.toString(field.getBoolean(obj));
                case "byte":
                    return Byte.toString(field.getByte(obj));
                case "short":
                    return Short.toString(field.getShort(obj));
                case "int":
                    return Integer.toString(field.getInt(obj));
                case "long":
                    return Long.toString(field.getLong(obj));
                case "float":
                    return Float.toString(field.getFloat(obj));
                case "double":
                    return Double.toString(field.getDouble(obj));
                case "String":
                    return (String) field.get(obj);
                default:
                    return "";
            }
        } catch (IllegalAccessException e) {
            LOG.warn("Access failed.", e);
        }
        return "";
    }

    /**
     * serialize sessionVariables different than default
     */
    private static JSONObject serializeChangedSessionVariable(SessionVariable sessionVariable,
                                                              SessionVariable newVar) throws IOException {
        JSONObject root = new JSONObject();
        try {
            for (Field field : SessionVariable.class.getDeclaredFields()) {
                VariableMgr.VarAttr attr = field.getAnnotation(VariableMgr.VarAttr.class);
                if (attr == null) {
                    continue;
                }
                field.setAccessible(true);
                if (getValue(sessionVariable, field).equals(getValue(newVar, field))) {
                    continue;
                }
                switch (field.getType().getSimpleName()) {
                    case "boolean":
                        root.put(attr.name(), field.get(sessionVariable));
                        break;
                    case "int":
                        root.put(attr.name(), field.get(sessionVariable));
                        break;
                    case "long":
                        root.put(attr.name(), field.get(sessionVariable));
                        break;
                    case "float":
                        root.put(attr.name(), field.get(sessionVariable));
                        break;
                    case "double":
                        root.put(attr.name(), field.get(sessionVariable));
                        break;
                    case "String":
                        root.put(attr.name(), field.get(sessionVariable));
                        break;
                    default:
                        // Unsupported type variable.
                        throw new IOException("invalid type: " + field.getType().getSimpleName());
                }
            }
        } catch (Exception e) {
            throw new IOException("failed to write session variable: " + e.getMessage());
        }
        return root;
    }

    /**
     * implementation of interface serializeInputsToDumpFile
     */
    private static JSONObject serializeInputs(Plan parsedPlan, Map<List<String>, TableIf> tables) throws IOException {
        ConnectContext connectContext = ConnectContext.get();
        // Create a JSON object
        JSONObject jsonObj = new JSONObject();
        String feVersion = FrontendsProcNode.getCurrentFrontendVersion(Env.getCurrentEnv()).getVersion()
                .substring(FE_VERSION_PREFIX_LENGTH);
        jsonObj.put("FeVersion", feVersion);
        String sql = connectContext.getStatementContext().getOriginStatement().originStmt;
        String sqlWithOutReplayCommand = sql.substring(14);
        jsonObj.put("Sql", sqlWithOutReplayCommand);
        // add session variable
        int beNumber = connectContext.getEnv().getClusterInfo().getBackendsNumber(true);
        connectContext.getSessionVariable().setBeNumberForTest(beNumber);
        SessionVariable newVar = new SessionVariable();
        jsonObj.put("SessionVariable", serializeChangedSessionVariable(connectContext.getSessionVariable(), newVar));
        jsonObj.put("CatalogMgr", GsonUtils.GSON.toJson(Env.getCurrentEnv().getInternalCatalog()));
        // add tables
        jsonObj.put("DbName", connectContext.getDatabase());
        JSONArray tablesJson = serializeTables(tables);
        jsonObj.put("Tables", tablesJson);
        // add colocate table index, used to indicate grouping of table distribution
        JSONObject colocateTableIndex = serializeColocateTableIndex(Env.getCurrentColocateIndex());
        jsonObj.put("ColocateTableIndex", colocateTableIndex);
        // add original sql, parsed plan and optimized plan
        jsonObj.put("ParsedPlan", ((AbstractPlan) parsedPlan).toJson());
        // Write the JSON object to a string and put it into file
        serializeStatsUsed(jsonObj, tables);
        return jsonObj;
    }

    /**
     * This function is used to serialize inputs of one query
     * @param parsedPlan input plan
     * @param tables all tables relative to this query
     * @throws IOException this will write to disk, so io exception should be dealed with
     */
    public static void serializeInputsToDumpFile(Plan parsedPlan, Map<List<String>, TableIf> tables)
            throws IOException {
        ConnectContext connectContext = ConnectContext.get();
        // when playing minidump file, we do not save input again.
        if (!isDump()) {
            return;
        }

        MinidumpUtils.init();
        connectContext.setMinidump(serializeInputs(parsedPlan, tables));
    }

    /**
     * get minidump string by query id, would find file in DUMP_PATH
     * @param queryId unique query id of a sql
     * @return minidump file content
     */
    public static String getMinidumpString(String queryId) {
        // Create a File object for the directory
        File directory = new File(DUMP_PATH);

        // Get all files in the directory
        File[] files = directory.listFiles();

        if (files != null) {
            for (File file : files) {
                // Check if the file name contains the search string
                if (file.isFile() && file.getName().contains(queryId)) {
                    try {
                        // Read the content of the file
                        String content = new String(Files.readAllBytes(Paths.get(file.getPath())));
                        // Add the content to the list
                        return content;
                    } catch (IOException e) {
                        break;
                    }
                }
            }
        }
        return null;
    }

    /**
     * init minidump utils before start to dump file, this will create a path
     */
    public static void init() {
        DUMP_PATH = Optional.ofNullable(DUMP_PATH).orElse(System.getenv("DORIS_HOME") + "/log/minidump");
        new File(DUMP_PATH).mkdirs();
    }
}
