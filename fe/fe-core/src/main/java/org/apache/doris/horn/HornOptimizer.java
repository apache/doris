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

package org.apache.doris.horn;


import org.apache.doris.common.UserException;
import org.apache.doris.horn.doris2horn.HornLogicalPlanThriftBuilder;
import org.apache.doris.horn.horn2doris.DorisPhysicalPlanBuilder;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;

import org.apache.horn4j.HornNative;
import org.apache.horn4j.thrift.TFlattenedExpression;
import org.apache.horn4j.thrift.THornOptimizeResult;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TSerializer;

/**
 * Doris-side entry to the Horn CBO optimizer.
 *
 * Flow: Doris LogicalPlan → TFlattenedExpression → horn4j JNI → Horn (in-process)
 *       → THornOptimizeResult → Doris PhysicalPlan.
 */
public class HornOptimizer {

    private static final Logger LOG = LogManager.getLogger(HornOptimizer.class);

    private final CascadesContext cascadesContext;
    private String hornExplainString;
    private String fallbackReason;

    public HornOptimizer(CascadesContext cascadesContext) {
        this.cascadesContext = cascadesContext;
    }

    public String getHornExplainString() {
        return hornExplainString;
    }

    public String getFallbackReason() {
        return fallbackReason;
    }

    public PhysicalPlan optimize(LogicalPlan logicalPlan) throws UserException {
        HornOptimizationContext hornCtx = new HornOptimizationContext(cascadesContext);

        try {
            // Step 1: Doris LogicalPlan → TFlattenedExpression
            LOG.info("Horn CBO: translating Doris plan to Horn input");

            HornLogicalPlanThriftBuilder planVisitor = new HornLogicalPlanThriftBuilder(hornCtx);
            TFlattenedExpression plan = planVisitor.translate(logicalPlan);

            TSerializer serializer = new TSerializer();
            byte[] planBytes = serializer.serialize(plan);
            byte[] tableBatchBytes = serializer.serialize(hornCtx.getTableBatch());
            byte[] queryOptionBytes = serializer.serialize(hornCtx.getQueryOptionConfig());

            // Step 2: call Horn in-process via JNI (horn4j).
            LOG.info("Horn CBO: calling Horn via horn4j JNI");

            byte[] resultBytes = new HornNative().optimize(
                    planBytes, tableBatchBytes, queryOptionBytes);

            // Step 3: Horn output → Doris PhysicalPlan.
            LOG.info("Horn CBO: rebuilding Doris plan from Horn output");

            THornOptimizeResult hornResult = new THornOptimizeResult();
            TDeserializer deserializer = new TDeserializer();
            deserializer.deserialize(hornResult, resultBytes);

            hornExplainString = hornResult.getExplain_string();
            if (hornExplainString != null) {
                LOG.info("Horn CBO explain:\n{}", hornExplainString);
            }

            DorisPhysicalPlanBuilder builder =
                    new DorisPhysicalPlanBuilder(hornCtx);
            PhysicalPlan physicalPlan = builder.build(hornResult.getTfexpr());

            LOG.info("Horn CBO: optimization complete");
            return physicalPlan;

        } catch (Exception e) {
            fallbackReason = e.getMessage();
            LOG.warn("Horn CBO failed, falling back to Nereids: {}", fallbackReason, e);
            return null;
        }
    }

}
