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
import org.apache.doris.catalog.TableIf;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.Histogram;

import org.json.JSONObject;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Minidump for Nereids */
public class Minidump {
    // traceflags

    // optimizer configuration
    private SessionVariable sessionVariable;

    // original sql
    private String sql;

    // parsed plan in json format
    private String parsedPlanJson;

    // result plan in json format
    private String resultPlanJson;

    private String dbName;

    // metadata objects
    private List<TableIf> tables;

    private Map<String, ColumnStatistic> totalColumnStatisticMap = new HashMap<>();

    private Map<String, Histogram> totalHistogramMap = new HashMap<>();

    private ColocateTableIndex colocateTableIndex;

    /** Minidump class used to save environment messages */
    public Minidump(String sql, SessionVariable sessionVariable,
                    String parsedPlanJson, String resultPlanJson, List<TableIf> tables,
                    String dbName, Map<String, ColumnStatistic> totalColumnStatisticMap,
                    Map<String, Histogram> totalHistogramMap, ColocateTableIndex colocateTableIndex) {
        this.sql = sql;
        this.sessionVariable = sessionVariable;
        this.parsedPlanJson = parsedPlanJson;
        this.resultPlanJson = resultPlanJson;
        this.tables = tables;
        this.dbName = dbName;
        this.totalColumnStatisticMap = totalColumnStatisticMap;
        this.totalHistogramMap = totalHistogramMap;
        this.colocateTableIndex = colocateTableIndex;
    }

    public SessionVariable getSessionVariable() {
        return sessionVariable;
    }

    public String getParsedPlanJson() {
        return parsedPlanJson;
    }

    public String getResultPlanJson() {
        return resultPlanJson;
    }

    public List<TableIf> getTables() {
        return tables;
    }

    public String getSql() {
        return sql;
    }

    public String getDbName() {
        return dbName;
    }

    public ColocateTableIndex getColocateTableIndex() {
        return colocateTableIndex;
    }

    public Map<String, ColumnStatistic> getTotalColumnStatisticMap() {
        return totalColumnStatisticMap;
    }

    public Map<String, Histogram> getTotalHistogramMap() {
        return totalHistogramMap;
    }

    /** Nereids minidump entry, argument should be absolute address of minidump path */
    public static void main(String[] args) {
        assert (args.length == 1);
        Minidump minidump = MinidumpUtils.loadMinidumpInputs(args[0]);
        JSONObject resultPlan = MinidumpUtils.executeSql(minidump.getSql());
        JSONObject minidumpResult = new JSONObject(minidump.getResultPlanJson());

        List<String> differences = MinidumpUtils.compareJsonObjects(minidumpResult, resultPlan, "");

        if (differences.isEmpty()) {
            System.out.println(minidump.sql);
        } else {
            System.out.println(minidump.sql + "\n" + "Result plan are not expected. Differences:");
            for (String difference : differences) {
                System.out.println(difference + "\n");
            }
        }
        System.exit(0);
    }
}
