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

import org.apache.doris.common.Config;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.nereids.cost.Cost;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupId;
import org.apache.doris.nereids.pattern.Pattern;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.plans.AbstractPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.qe.ConnectContext;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

/**
 * log consumer
 */
public class NereidsTracer {

    private static final Logger LOG = LogManager.getLogger(NereidsTracer.class);

    private static long startTime;
    private static String TRACE_PATH = null;

    private static boolean shouldLog = false;

    private static JSONObject totalTraces = new JSONObject();

    private static JSONArray sortedTraces = new JSONArray();

    private static JSONArray enforcerEvent = new JSONArray();

    private static JSONArray rewriteEvent = new JSONArray();

    private static JSONArray applyRuleEvent = new JSONArray();

    private static JSONArray propertyAndCostEvent = new JSONArray();

    private static JSONArray importantTime = new JSONArray();

    private static void saveSorted(JSONObject timeEvent) {
        sortedTraces.put(timeEvent);
    }

    public static String getCurrentTime() {
        return TimeUtils.getElapsedTimeMs(NereidsTracer.startTime) + "ms";
    }

    /** log rewrite event when open switch */
    public static void logRewriteEvent(String rule, Pattern<? extends Plan> pattern, Plan inputPlan, Plan outputPlan) {
        if (!shouldLog) {
            return;
        }
        JSONObject rewriteEventJson = new JSONObject();
        JSONObject rewriteMsg = new JSONObject();
        rewriteMsg.put("RuleType", rule);
        rewriteMsg.put("Input", ((AbstractPlan) inputPlan).toJson());
        rewriteMsg.put("Output", ((AbstractPlan) outputPlan).toJson());
        rewriteEventJson.put(getCurrentTime(), rewriteMsg);
        rewriteEvent.put(rewriteEventJson);
        saveSorted(rewriteEventJson);
    }

    /** log apply rule event when doing transformation both exploration and implementation */
    public static void logApplyRuleEvent(String rule, Plan inputPlan, Plan outputPlan) {
        if (!shouldLog) {
            return;
        }
        JSONObject applyRuleEventJson = new JSONObject();
        JSONObject rewriteMsg = new JSONObject();
        rewriteMsg.put("RuleType", rule);
        rewriteMsg.put("Input", ((AbstractPlan) inputPlan).toJson());
        rewriteMsg.put("Output", ((AbstractPlan) outputPlan).toJson());
        applyRuleEventJson.put(getCurrentTime(), rewriteMsg);
        applyRuleEvent.put(applyRuleEventJson);
        saveSorted(applyRuleEventJson);
    }

    /** log property and cost pair when doing cost and enforcer task */
    public static void logPropertyAndCostEvent(
            GroupId groupId, List<Group> children, Plan plan, PhysicalProperties requestProperty, Cost cost) {
        if (!shouldLog) {
            return;
        }
        JSONObject propertyAndCostJson = new JSONObject();
        JSONObject propertyAndCost = new JSONObject();
        String groupMsg = groupId + " -> ";
        for (Group child : children) {
            groupMsg = groupMsg + child.getGroupId() + "/";
        }
        propertyAndCost.put("GroupId", groupMsg);
        propertyAndCost.put("Plan", ((AbstractPlan) plan).toJson());
        propertyAndCost.put("PhysicalProperties", requestProperty.toString());
        propertyAndCost.put("Cost", cost.toString());
        propertyAndCostJson.put(getCurrentTime(), propertyAndCost);
        propertyAndCostEvent.put(propertyAndCostJson);
        saveSorted(propertyAndCostJson);
    }

    /** log enforcer event when we need to add enforcer */
    public static void logEnforcerEvent(
            GroupId groupId, Plan plan, PhysicalProperties inputProperty, PhysicalProperties outputProperty) {
        if (!shouldLog) {
            return;
        }
        JSONObject enforcerEventJson = new JSONObject();
        JSONObject enforcerMsg = new JSONObject();
        enforcerMsg.put("GroupId", groupId.toString());
        enforcerMsg.put("Plan", ((AbstractPlan) plan).toJson());
        enforcerMsg.put("InputProperty", inputProperty.toString());
        enforcerMsg.put("OutputProperty", outputProperty.toString());
        enforcerEventJson.put(getCurrentTime(), enforcerMsg);
        enforcerEvent.put(enforcerEventJson);
        saveSorted(enforcerEventJson);
    }

    /** log important time of nereids optimize process, like analyze rewrite */
    public static void logImportantTime(String eventDesc) {
        if (!shouldLog) {
            return;
        }
        JSONObject timeEvent = new JSONObject();
        timeEvent.put(getCurrentTime(), eventDesc);
        importantTime.put(timeEvent);
        saveSorted(timeEvent);
    }

    /** ouput of tracer, just after optimize process */
    public static void output(ConnectContext connectContext) {
        if (!shouldLog) {
            return;
        }
        String queryId = (connectContext.queryId() == null)
                ? "traceDemo" : DebugUtil.printId(connectContext.queryId());
        totalTraces.put("ImportantTime", importantTime);
        totalTraces.put("RewriteEvent", rewriteEvent);
        totalTraces.put("ApplyRuleEvent", applyRuleEvent);
        totalTraces.put("PropertyAndCostPairs", propertyAndCostEvent);
        totalTraces.put("EnforcerEvent", enforcerEvent);
        try (FileWriter file = new FileWriter(TRACE_PATH + "/" + queryId + ".json")) {
            file.write(totalTraces.toString(4));
        } catch (IOException e) {
            LOG.info("failed to output of tracer", e);
        }
    }

    /** initialize of nereids tracer */
    public static void init() {
        NereidsTracer.shouldLog = true;
        startTime = TimeUtils.getStartTimeMs();
        TRACE_PATH = Optional.ofNullable(TRACE_PATH).orElse(Config.nereids_trace_log_dir);
        new File(TRACE_PATH).mkdirs();
    }

    public static void disable() {
        NereidsTracer.shouldLog = false;
    }
}
