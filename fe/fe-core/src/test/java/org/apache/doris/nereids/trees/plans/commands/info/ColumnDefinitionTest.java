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

package org.apache.doris.nereids.trees.plans.commands.info;

import org.apache.doris.analysis.ColumnDef;
import org.apache.doris.analysis.TypeDef;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.GeographyType;
import org.apache.doris.nereids.types.GeometryType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Optional;

class ColumnDefinitionTest {
    private static final String INTERNAL_TABLE_SPATIAL_ERROR =
            "GEOMETRY and GEOGRAPHY are not supported for Doris internal tables";

    @Test
    void rejectSpatialTypesForInternalTables() {
        for (DataType type : new DataType[] {new GeometryType("EPSG:3857"),
                new GeographyType("OGC:CRS84", "spherical")}) {
            ColumnDefinition definition =
                    new ColumnDefinition("shape", type, false, null, true, Optional.empty(), "");
            AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                    () -> definition.validate(true, Collections.emptySet(), Collections.emptySet(), false,
                            KeysType.DUP_KEYS));
            Assertions.assertTrue(exception.getMessage().contains(INTERNAL_TABLE_SPATIAL_ERROR));
        }
    }

    @Test
    void rejectSpatialTypesForInternalTablesInLegacyPlanner() {
        for (ScalarType type : new ScalarType[] {ScalarType.createGeometryType("EPSG:3857"),
                ScalarType.createGeographyType("OGC:CRS84", "spherical")}) {
            ColumnDef definition = new ColumnDef("shape", new TypeDef(type), false, null, true,
                    ColumnDef.DefaultValue.NOT_SET, "");
            org.apache.doris.common.AnalysisException exception = Assertions.assertThrows(
                    org.apache.doris.common.AnalysisException.class, () -> definition.analyze(true));
            Assertions.assertTrue(exception.getMessage().contains(INTERNAL_TABLE_SPATIAL_ERROR));
        }
    }

    @Test
    void rejectNestedSpatialTypesForInternalTables() {
        ColumnDefinition nereidsDefinition = new ColumnDefinition("shapes",
                ArrayType.of(new GeometryType("EPSG:3857")), false, null, true, Optional.empty(), "");
        AnalysisException nereidsException = Assertions.assertThrows(AnalysisException.class,
                () -> nereidsDefinition.validate(true, Collections.emptySet(), Collections.emptySet(), false,
                        KeysType.DUP_KEYS));
        Assertions.assertTrue(nereidsException.getMessage().contains(INTERNAL_TABLE_SPATIAL_ERROR));

        ColumnDef legacyDefinition = new ColumnDef("shapes", new TypeDef(
                new org.apache.doris.catalog.ArrayType(ScalarType.createGeometryType("EPSG:3857"))),
                false, null, true, ColumnDef.DefaultValue.NOT_SET, "");
        org.apache.doris.common.AnalysisException legacyException = Assertions.assertThrows(
                org.apache.doris.common.AnalysisException.class, () -> legacyDefinition.analyze(true));
        Assertions.assertTrue(legacyException.getMessage().contains(INTERNAL_TABLE_SPATIAL_ERROR));
    }
}
