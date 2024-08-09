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
import org.apache.doris.nereids.trees.plans.logical.LogicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalDeferMaterializeOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalEsScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalExternalRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalHudiScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJdbcScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOdbcScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalSchemaScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalTVFRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalTestScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDeferMaterializeOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalEsScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFileScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHudiScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalJdbcScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOdbcScan;
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

    default R visitLogicalCatalogRelation(LogicalCatalogRelation catalogRelation, C context) {
        return visitLogicalRelation(catalogRelation, context);
    }

    R visitPhysicalRelation(PhysicalRelation physicalRelation, C context);

    default R visitPhysicalCatalogRelation(PhysicalCatalogRelation catalogRelation, C context) {
        return visitPhysicalRelation(catalogRelation, context);
    }

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

    default R visitLogicalExternalRelation(LogicalExternalRelation relation, C context) {
        return visitLogicalCatalogRelation(relation, context);
    }

    default R visitLogicalEsScan(LogicalEsScan esScan, C context) {
        return visitLogicalExternalRelation(esScan, context);
    }

    default R visitLogicalFileScan(LogicalFileScan fileScan, C context) {
        return visitLogicalExternalRelation(fileScan, context);
    }

    default R visitLogicalHudiScan(LogicalHudiScan fileScan, C context) {
        return visitLogicalFileScan(fileScan, context);
    }

    default R visitLogicalJdbcScan(LogicalJdbcScan jdbcScan, C context) {
        return visitLogicalExternalRelation(jdbcScan, context);
    }

    default R visitLogicalOdbcScan(LogicalOdbcScan odbcScan, C context) {
        return visitLogicalExternalRelation(odbcScan, context);
    }

    default R visitLogicalOlapScan(LogicalOlapScan olapScan, C context) {
        return visitLogicalCatalogRelation(olapScan, context);
    }

    default R visitLogicalDeferMaterializeOlapScan(
            LogicalDeferMaterializeOlapScan deferMaterializeOlapScan, C context) {
        return visitLogicalCatalogRelation(deferMaterializeOlapScan, context);
    }

    default R visitLogicalOneRowRelation(LogicalOneRowRelation oneRowRelation, C context) {
        return visitLogicalRelation(oneRowRelation, context);
    }

    default R visitLogicalSchemaScan(LogicalSchemaScan schemaScan, C context) {
        return visitLogicalCatalogRelation(schemaScan, context);
    }

    default R visitLogicalTVFRelation(LogicalTVFRelation tvfRelation, C context) {
        return visitLogicalRelation(tvfRelation, context);
    }

    default R visitLogicalTestScan(LogicalTestScan testScan, C context) {
        return visitLogicalCatalogRelation(testScan, context);
    }

    // *******************************
    // physical relations
    // *******************************

    default R visitPhysicalEmptyRelation(PhysicalEmptyRelation emptyRelation, C context) {
        return visitPhysicalRelation(emptyRelation, context);
    }

    default R visitPhysicalEsScan(PhysicalEsScan esScan, C context) {
        return visitPhysicalCatalogRelation(esScan, context);
    }

    default R visitPhysicalFileScan(PhysicalFileScan fileScan, C context) {
        return visitPhysicalCatalogRelation(fileScan, context);
    }

    default R visitPhysicalHudiScan(PhysicalHudiScan hudiScan, C context) {
        return visitPhysicalFileScan(hudiScan, context);
    }

    default R visitPhysicalJdbcScan(PhysicalJdbcScan jdbcScan, C context) {
        return visitPhysicalCatalogRelation(jdbcScan, context);
    }

    default R visitPhysicalOdbcScan(PhysicalOdbcScan odbcScan, C context) {
        return visitPhysicalCatalogRelation(odbcScan, context);
    }

    default R visitPhysicalOlapScan(PhysicalOlapScan olapScan, C context) {
        return visitPhysicalCatalogRelation(olapScan, context);
    }

    default R visitPhysicalDeferMaterializeOlapScan(
            PhysicalDeferMaterializeOlapScan deferMaterializeOlapScan, C context) {
        return visitPhysicalCatalogRelation(deferMaterializeOlapScan, context);
    }

    default R visitPhysicalOneRowRelation(PhysicalOneRowRelation oneRowRelation, C context) {
        return visitPhysicalRelation(oneRowRelation, context);
    }

    default R visitPhysicalSchemaScan(PhysicalSchemaScan schemaScan, C context) {
        return visitPhysicalCatalogRelation(schemaScan, context);
    }

    default R visitPhysicalTVFRelation(PhysicalTVFRelation tvfRelation, C context) {
        return visitPhysicalRelation(tvfRelation, context);
    }

    default R visitPhysicalCTEConsumer(PhysicalCTEConsumer consumer, C context) {
        return visitPhysicalRelation(consumer, context);
    }
}
