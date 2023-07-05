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
import org.apache.doris.catalog.SchemaTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.nereids.trees.plans.AbstractPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.VariableMgr;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.ColumnStatisticBuilder;
import org.apache.doris.statistics.Histogram;

import org.json.JSONArray;
import org.json.JSONObject;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Field;
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

    public static String DUMP_PATH = null;

    /**
     * Saving of minidump file to fe log path
     */
    public static void saveMinidumpString(JSONObject minidump, String dumpName) {
        String dumpPath = MinidumpUtils.DUMP_PATH + "/" + dumpName;
        File minidumpFileDir = new File(dumpPath);
        if (!minidumpFileDir.exists()) {
            minidumpFileDir.mkdirs();
        }
        String jsonMinidump = minidump.toString(4);
        try (FileWriter file = new FileWriter(dumpPath + "/" + "dumpFile.json")) {
            file.write(jsonMinidump);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Loading of minidump file
     */
    public static Minidump jsonMinidumpLoad(String dumpFilePath) throws IOException {
        // open file, read file, put them into minidump object
        try (FileInputStream inputStream = new FileInputStream(dumpFilePath + "/dumpFile.json")) {
            StringBuilder sb = new StringBuilder();
            int ch;
            while ((ch = inputStream.read()) != -1) {
                sb.append((char) ch);
            }
            String inputString = sb.toString();
            // Parse the JSON string back into a JSON object
            JSONObject inputJSON = new JSONObject(inputString);
            SessionVariable newSessionVariable = new SessionVariable();
            newSessionVariable.readFromJson(inputJSON.getString("SessionVariable"));
            String sql = inputJSON.getString("Sql");

            List<TableIf> tables = new ArrayList<>();
            String catalogName = inputJSON.getString("CatalogName");
            String dbName = inputJSON.getString("DbName");
            JSONArray tablesJson = (JSONArray) inputJSON.get("Tables");
            for (int i = 0; i < tablesJson.length(); i++) {
                String tablePath = dumpFilePath + (String) tablesJson.get(i);
                DataInputStream dis = new DataInputStream(new FileInputStream(tablePath));
                Table newTable = Table.read(dis);
                tables.add(newTable);
            }
            String colocateTableIndexPath = dumpFilePath + inputJSON.getString("ColocateTableIndex");
            DataInputStream dis = new DataInputStream(new FileInputStream(colocateTableIndexPath));
            ColocateTableIndex newColocateTableIndex = new ColocateTableIndex();
            newColocateTableIndex.readFields(dis);

            JSONArray columnStats = (JSONArray) inputJSON.get("ColumnStatistics");
            Map<String, ColumnStatistic> columnStatisticMap = new HashMap<>();
            for (int i = 0; i < columnStats.length(); i++) {
                JSONObject oneColumnStat = (JSONObject) columnStats.get(i);
                String colName = oneColumnStat.keys().next();
                String colStat = oneColumnStat.getString(colName);
                ColumnStatistic columnStatistic = ColumnStatistic.fromJson(colStat);
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

            return new Minidump(sql, newSessionVariable, parsedPlanJson, resultPlanJson,
                    tables, catalogName, dbName, columnStatisticMap, histogramMap, newColocateTableIndex);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * serialize tables from Table in catalog to json format
     */
    public static JSONArray serializeTables(
            String minidumpFileDir, String dbAndCatalogName, List<TableIf> tables) throws IOException {
        JSONArray tablesJson = new JSONArray();
        for (TableIf table : tables) {
            if (table instanceof SchemaTable) {
                continue;
            }
            String tableFileName = dbAndCatalogName + table.getName();
            tablesJson.put(tableFileName);
            DataOutputStream dos = new DataOutputStream(new FileOutputStream(minidumpFileDir + tableFileName));
            table.write(dos);
            dos.flush();
            dos.close();
        }
        return tablesJson;
    }

    public static void serializeColocateTableIndex(
            String colocateTableIndexFile, ColocateTableIndex colocateTableIndex) throws IOException {
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(colocateTableIndexFile));
        colocateTableIndex.write(dos);
        dos.flush();
        dos.close();
    }

    private static ColumnStatistic getColumnStatistic(TableIf table, String colName) {
        return Env.getCurrentEnv().getStatisticsCache().getColumnStatistics(
            table.getDatabase().getCatalog().getId(), table.getDatabase().getId(), table.getId(), colName);
    }

    private static Histogram getColumnHistogram(TableIf table, String colName) {
        return Env.getCurrentEnv().getStatisticsCache().getHistogram(table.getId(), colName);
    }

    /**
     * serialize column statistic and histograms when loading to dumpfile and environment
     */
    private static void serializeStatsUsed(JSONObject jsonObj, List<Table> tables) {
        JSONArray columnStatistics = new JSONArray();
        JSONArray histograms = new JSONArray();
        for (Table table : tables) {
            if (table instanceof SchemaTable) {
                continue;
            }
            List<Column> columns = table.getColumns();
            for (Column column : columns) {
                String colName = column.getName();
                ColumnStatistic cache =
                        Config.enable_stats ? getColumnStatistic(table, colName) : ColumnStatistic.UNKNOWN;
                if (cache.avgSizeByte <= 0) {
                    cache = new ColumnStatisticBuilder(cache)
                        .setAvgSizeByte(column.getType().getSlotSize())
                        .build();
                }

                Histogram histogram = getColumnHistogram(table, colName);
                if (histogram != null) {
                    JSONObject oneHistogram = new JSONObject();
                    oneHistogram.put(colName, Histogram.serializeToJson(histogram));
                    histograms.put(oneHistogram);
                }
                JSONObject oneColumnStats = new JSONObject();
                oneColumnStats.put(colName, cache.toJson());
                columnStatistics.put(oneColumnStats);
            }
        }
        jsonObj.put("ColumnStatistics", columnStatistics);
        jsonObj.put("Histogram", histograms);
    }

    /**
     * serialize output plan to dump file and persistent into disk
     * @param resultPlan
     *
     */
    public static void serializeOutputToDumpFile(Plan resultPlan) {
        if (ConnectContext.get().getSessionVariable().isPlayNereidsDump()
                || !ConnectContext.get().getSessionVariable().isEnableMinidump()) {
            return;
        }
        ConnectContext.get().getMinidump().put("ResultPlan", ((AbstractPlan) resultPlan).toJson());
        if (ConnectContext.get().getSessionVariable().isEnableMinidump()) {
            saveMinidumpString(ConnectContext.get().getMinidump(),
                    DebugUtil.printId(ConnectContext.get().queryId()));
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

    /**
     * serialize sessionVariables different than default
     */
    private static JSONObject serializeChangedSessionVariable(SessionVariable sessionVariable) throws IOException {
        JSONObject root = new JSONObject();
        try {
            for (Field field : SessionVariable.class.getDeclaredFields()) {
                VariableMgr.VarAttr attr = field.getAnnotation(VariableMgr.VarAttr.class);
                if (attr == null) {
                    continue;
                }
                field.setAccessible(true);
                if (field.get(sessionVariable).equals(field.get(VariableMgr.getDefaultSessionVariable()))) {
                    continue;
                }
                switch (field.getType().getSimpleName()) {
                    case "boolean":
                        root.put(attr.name(), (Boolean) field.get(sessionVariable));
                        break;
                    case "int":
                        root.put(attr.name(), (Integer) field.get(sessionVariable));
                        break;
                    case "long":
                        root.put(attr.name(), (Long) field.get(sessionVariable));
                        break;
                    case "float":
                        root.put(attr.name(), (Float) field.get(sessionVariable));
                        break;
                    case "double":
                        root.put(attr.name(), (Double) field.get(sessionVariable));
                        break;
                    case "String":
                        root.put(attr.name(), (String) field.get(sessionVariable));
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
    private static JSONObject serializeInputs(Plan parsedPlan, List<Table> tables, String dumpName) throws IOException {
        String dumpPath = MinidumpUtils.DUMP_PATH + "/" + dumpName;
        File minidumpFileDir = new File(dumpPath);
        if (!minidumpFileDir.exists()) {
            minidumpFileDir.mkdirs();
        }
        // Create a JSON object
        JSONObject jsonObj = new JSONObject();
        jsonObj.put("Sql", ConnectContext.get().getStatementContext().getOriginStatement().originStmt);
        // add session variable
        jsonObj.put("SessionVariable", serializeChangedSessionVariable(ConnectContext.get().getSessionVariable()));
        // add tables
        String dbAndCatalogName = "/" + ConnectContext.get().getDatabase() + "-"
                + ConnectContext.get().getCurrentCatalog().getName() + "-";
        jsonObj.put("CatalogName", ConnectContext.get().getCurrentCatalog().getName());
        jsonObj.put("DbName", ConnectContext.get().getDatabase());
        JSONArray tablesJson = MinidumpUtils.serializeTables(dumpPath, dbAndCatalogName, tables);
        jsonObj.put("Tables", tablesJson);
        // add colocate table index, used to indicate grouping of table distribution
        String colocateTableIndexPath = dumpPath + "/ColocateTableIndex";
        MinidumpUtils.serializeColocateTableIndex(colocateTableIndexPath, Env.getCurrentColocateIndex());
        jsonObj.put("ColocateTableIndex", "/ColocateTableIndex");
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
    public static void serializeInputsToDumpFile(Plan parsedPlan, List<Table> tables) throws IOException {
        // when playing minidump file, we do not save input again.
        if (ConnectContext.get().getSessionVariable().isPlayNereidsDump()
                || !ConnectContext.get().getSessionVariable().isEnableMinidump()) {
            return;
        }

        if (!ConnectContext.get().getSessionVariable().getMinidumpPath().equals("default")) {
            MinidumpUtils.DUMP_PATH = ConnectContext.get().getSessionVariable().getMinidumpPath();
        }
        MinidumpUtils.init();
        String queryId = DebugUtil.printId(ConnectContext.get().queryId());
        ConnectContext.get().setMinidump(serializeInputs(parsedPlan, tables, queryId));
    }

    /**
     * init minidump utils before start to dump file, this will create a path
     */
    public static void init() {
        DUMP_PATH = Optional.ofNullable(DUMP_PATH).orElse(System.getenv("DORIS_HOME") + "/log/minidump");
        new File(DUMP_PATH).mkdirs();
    }
}
