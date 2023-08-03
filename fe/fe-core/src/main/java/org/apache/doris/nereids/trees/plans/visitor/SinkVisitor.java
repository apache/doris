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

package org.apache.doris.nereids.trees.plans.visitor;

import org.apache.doris.nereids.analyzer.UnboundOlapTableSink;
import org.apache.doris.nereids.analyzer.UnboundResultSink;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalDeferMaterializeResultSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapTableSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalResultSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalSink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDeferMaterializeResultSink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFileSink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapTableSink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalResultSink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSink;

/**
 * sink visitor
 */
public interface SinkVisitor<R, C> {

    // *******************************
    // interface
    // *******************************

    R visitLogicalSink(LogicalSink<? extends Plan> logicalSink, C context);

    R visitPhysicalSink(PhysicalSink<? extends Plan> physicalSink, C context);

    // *******************************
    // unbound
    // *******************************

    default R visitUnboundOlapTableSink(UnboundOlapTableSink<? extends Plan> unboundOlapTableSink, C context) {
        return visitLogicalSink(unboundOlapTableSink, context);
    }

    default R visitUnboundResultSink(UnboundResultSink<? extends Plan> unboundResultSink, C context) {
        return visitLogicalSink(unboundResultSink, context);
    }

    // *******************************
    // logical
    // *******************************

    default R visitLogicalFileSink(LogicalFileSink<? extends Plan> fileSink, C context) {
        return visitLogicalSink(fileSink, context);
    }

    default R visitLogicalOlapTableSink(LogicalOlapTableSink<? extends Plan> olapTableSink, C context) {
        return visitLogicalSink(olapTableSink, context);
    }

    default R visitLogicalResultSink(LogicalResultSink<? extends Plan> logicalResultSink, C context) {
        return visitLogicalSink(logicalResultSink, context);
    }

    default R visitLogicalDeferMaterializeResultSink(
            LogicalDeferMaterializeResultSink<? extends Plan> logicalDeferMaterializeResultSink, C context) {
        return visitLogicalSink(logicalDeferMaterializeResultSink, context);
    }

    // *******************************
    // physical
    // *******************************

    default R visitPhysicalFileSink(PhysicalFileSink<? extends Plan> fileSink, C context) {
        return visitPhysicalSink(fileSink, context);
    }

    default R visitPhysicalOlapTableSink(PhysicalOlapTableSink<? extends Plan> olapTableSink, C context) {
        return visitPhysicalSink(olapTableSink, context);
    }

    default R visitPhysicalResultSink(PhysicalResultSink<? extends Plan> physicalResultSink, C context) {
        return visitPhysicalSink(physicalResultSink, context);
    }

    default R visitPhysicalDeferMaterializeResultSink(
            PhysicalDeferMaterializeResultSink<? extends Plan> sink, C context) {
        return visitPhysicalSink(sink, context);
    }
}
