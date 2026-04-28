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

package org.apache.doris.common.profile;

import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.system.SystemInfoService;

import com.google.common.base.Strings;
import hu.webarticum.treeprinter.BorderTreeNodeDecorator;
import hu.webarticum.treeprinter.SimpleTreeNode;
import hu.webarticum.treeprinter.TraditionalTreePrinter;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.HashSet;
import java.util.Set;

public class PlanTreePrinter {

    private static final String DELIMITER = "||";

    public static String printPlanExplanation(PlanTreeNode root) {
        SimpleTreeNode rootNode = buildNode(root);
        StringBuilder sb = new StringBuilder();
        new TraditionalTreePrinter().print(new BorderTreeNodeDecorator(rootNode), sb);
        if (isMaskingLineagePrintEnabled()) {
            return appendLineageOutput(sb.toString());
        } else {
            return sb.toString();
        }
    }

    public static String printPlanExplanation(String explain) {
        if (isMaskingLineagePrintEnabled()) {
            return appendLineageOutput(explain);
        } else {
            return explain;
        }
    }

    private static SimpleTreeNode buildNode(PlanTreeNode planNode) {
        SimpleTreeNode node = new SimpleTreeNode(planNode.getExplainStr());
        for (PlanTreeNode child : planNode.getChildren()) {
            node.addChild(buildNode(child));
        }
        return node;
    }

    public static String printPlanTree(PlanTreeNode root) {
        if (isMaskingLineagePrintEnabled()) {
            return appendLineageOutput(buildTree(root, ""));
        } else {
            return buildTree(root, "");
        }
    }

    private static String buildTree(PlanTreeNode planNode, String prefix) {
        StringBuilder builder = new StringBuilder();
        builder.append(prefix).append(planNode.getIds()).append(":")
                .append(planNode.getExplainStr().replaceAll("\n", DELIMITER)).append("\n");
        String childPrefix = prefix + "--";
        planNode.getChildren().forEach(
                child -> {
                    builder.append(buildTree(child, childPrefix));
                }
        );
        return builder.toString();
    }

    private static boolean isMaskingLineagePrintEnabled() {
        ConnectContext context = ConnectContext.get();
        SessionVariable sessionVariable = context == null ? null : context.getSessionVariable();
        return sessionVariable != null && sessionVariable.maskingLineagePrintEnabled;
    }

    private static String appendLineageOutput(String explain) {
        ConnectContext context = ConnectContext.get();
        SessionVariable sessionVariable = context == null ? null : context.getSessionVariable();
        StmtExecutor executor = context == null ? null : context.getExecutor();
        if (executor == null || executor.planner() == null) {
            return explain;
        }

        if (executor.planner().getScanNodes() == null || executor.planner().getScanNodes().isEmpty()) {
            return explain;
        }

        JSONArray inputTableList = new JSONArray();
        Set<String> dedup = new HashSet<>();
        for (ScanNode scanNode : executor.planner().getScanNodes()) {
            TupleDescriptor tupleDescriptor = scanNode.getTupleDesc();
            if (tupleDescriptor == null || tupleDescriptor.getTable() == null) {
                continue;
            }
            TableIf table = tupleDescriptor.getTable();
            DatabaseIf<?> database = table.getDatabase();
            String dbFullName = database != null ? database.getFullName() : "";
            String tableName = table.getName();
            String clusterName = SystemInfoService.DEFAULT_CLUSTER;
            String dbName = dbFullName;
            int clusterDelimiterIndex = dbFullName.indexOf(':');
            if (clusterDelimiterIndex > 0 && clusterDelimiterIndex + 1 < dbFullName.length()) {
                clusterName = dbFullName.substring(0, clusterDelimiterIndex);
                dbName = dbFullName.substring(clusterDelimiterIndex + 1);
            }
            if (Strings.isNullOrEmpty(dbName) || Strings.isNullOrEmpty(tableName)) {
                continue;
            }
            String key = clusterName + "|" + dbName + "|" + tableName;
            if (!dedup.add(key)) {
                continue;
            }
            JSONObject tableInfo = new JSONObject();
            tableInfo.put("tbName", tableName);
            tableInfo.put("dbName", dbName);
            tableInfo.put("msCluster", clusterName);
            inputTableList.put(tableInfo);
        }

        JSONObject lineage = new JSONObject();
        lineage.put("inputTableList", inputTableList);
        String lineageJson = lineage.toString();

        String prefix = sessionVariable == null ? "" : Strings.nullToEmpty(sessionVariable.maskingLineagePrintPrefix);
        String suffix = sessionVariable == null ? "" : Strings.nullToEmpty(sessionVariable.maskingLineagePrintSuffix);
        StringBuilder sb = new StringBuilder(explain.length() + lineageJson.length()
                + prefix.length() + suffix.length() + 2);
        sb.append(explain);
        if (!explain.endsWith("\n")) {
            sb.append("\n");
        }
        sb.append(prefix).append(lineageJson).append(suffix);
        return sb.toString();
    }
}
