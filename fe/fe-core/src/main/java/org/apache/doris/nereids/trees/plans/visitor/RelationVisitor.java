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

import org.apache.doris.nereids.analyzer.UnboundOneRowRelation;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.analyzer.UnboundTVFRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalDeferMaterializeOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalEsScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJdbcScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalSchemaScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalTVFRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDeferMaterializeOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalEsScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFileScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalJdbcScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSchemaScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalTVFRelation;

/**
 * relation visitor
 */
public interface RelationVisitor<R, C> {

    // *******************************
    // interface
    // *******************************

    R visitLogicalRelation(LogicalRelation logicalRelation, C context);

    R visitPhysicalRelation(PhysicalRelation physicalRelation, C context);

    // *******************************
    // unbound relations
    // *******************************

    default R visitUnboundOneRowRelation(UnboundOneRowRelation oneRowRelation, C context) {
        return visitLogicalRelation(oneRowRelation, context);
    }

    default R visitUnboundRelation(UnboundRelation relation, C context) {
        return visitLogicalRelation(relation, context);
    }

    default R visitUnboundTVFRelation(UnboundTVFRelation unboundTVFRelation, C context) {
        return visitLogicalRelation(unboundTVFRelation, context);
    }

    // *******************************
    // logical relations
    // *******************************

    default R visitLogicalEmptyRelation(LogicalEmptyRelation emptyRelation, C context) {
        return visitLogicalRelation(emptyRelation, context);
    }

    default R visitLogicalEsScan(LogicalEsScan esScan, C context) {
        return visitLogicalRelation(esScan, context);
    }

    default R visitLogicalFileScan(LogicalFileScan fileScan, C context) {
        return visitLogicalRelation(fileScan, context);
    }

    default R visitLogicalJdbcScan(LogicalJdbcScan jdbcScan, C context) {
        return visitLogicalRelation(jdbcScan, context);
    }

    default R visitLogicalOlapScan(LogicalOlapScan olapScan, C context) {
        return visitLogicalRelation(olapScan, context);
    }

    default R visitLogicalDeferMaterializeOlapScan(
            LogicalDeferMaterializeOlapScan deferMaterializeOlapScan, C context) {
        return visitLogicalRelation(deferMaterializeOlapScan, context);
    }

    default R visitLogicalOneRowRelation(LogicalOneRowRelation oneRowRelation, C context) {
        return visitLogicalRelation(oneRowRelation, context);
    }

    default R visitLogicalSchemaScan(LogicalSchemaScan schemaScan, C context) {
        return visitLogicalRelation(schemaScan, context);
    }

    default R visitLogicalTVFRelation(LogicalTVFRelation tvfRelation, C context) {
        return visitLogicalRelation(tvfRelation, context);
    }

    // *******************************
    // physical relations
    // *******************************

    default R visitPhysicalEmptyRelation(PhysicalEmptyRelation emptyRelation, C context) {
        return visitPhysicalRelation(emptyRelation, context);
    }

    default R visitPhysicalEsScan(PhysicalEsScan esScan, C context) {
        return visitPhysicalRelation(esScan, context);
    }

    default R visitPhysicalFileScan(PhysicalFileScan fileScan, C context) {
        return visitPhysicalRelation(fileScan, context);
    }

    default R visitPhysicalJdbcScan(PhysicalJdbcScan jdbcScan, C context) {
        return visitPhysicalRelation(jdbcScan, context);
    }

    default R visitPhysicalOlapScan(PhysicalOlapScan olapScan, C context) {
        return visitPhysicalRelation(olapScan, context);
    }

    default R visitPhysicalDeferMaterializeOlapScan(
            PhysicalDeferMaterializeOlapScan deferMaterializeOlapScan, C context) {
        return visitPhysicalRelation(deferMaterializeOlapScan, context);
    }

    default R visitPhysicalOneRowRelation(PhysicalOneRowRelation oneRowRelation, C context) {
        return visitPhysicalRelation(oneRowRelation, context);
    }

    default R visitPhysicalSchemaScan(PhysicalSchemaScan schemaScan, C context) {
        return visitPhysicalRelation(schemaScan, context);
    }

    default R visitPhysicalTVFRelation(PhysicalTVFRelation tvfRelation, C context) {
        return visitPhysicalRelation(tvfRelation, context);
    }
}
