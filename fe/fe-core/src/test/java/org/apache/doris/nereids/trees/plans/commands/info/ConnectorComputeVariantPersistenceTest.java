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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.View;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.types.ConnectorComputeVariantType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.VariantType;
import org.apache.doris.persist.gson.GsonUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

class ConnectorComputeVariantPersistenceTest {

    @Test
    void ctasAndMtmvConversionProducesReplayableCatalogVariant() {
        DataType persistedType = ConnectorComputeVariantType.INSTANCE.conversion();
        Assertions.assertInstanceOf(VariantType.class, persistedType);
        Assertions.assertFalse(persistedType instanceof ConnectorComputeVariantType);

        Column column = new ColumnDefinition("payload", persistedType, true, null)
                .translateToCatalogStyle();
        String json = GsonUtils.GSON.toJson(column);
        Column replayed = GsonUtils.GSON.fromJson(json, Column.class);
        Assertions.assertInstanceOf(org.apache.doris.catalog.VariantType.class,
                replayed.getType());
        Assertions.assertFalse(replayed.getType()
                instanceof org.apache.doris.datasource.connector.converter.ConnectorComputeVariantType);
    }

    @Test
    void viewSchemaRoundTripDoesNotPersistExecutionMarker() throws Exception {
        TestViewInfo info = new TestViewInfo();
        Slot output = SlotReference.of("payload", ConnectorComputeVariantType.INSTANCE);
        info.createColumns(Collections.singletonList(output));

        View view = new View(1L, "variant_view", info.finalCols);
        String json = GsonUtils.GSON.toJson(view);
        View replayed = GsonUtils.GSON.fromJson(json, View.class);
        Assertions.assertInstanceOf(org.apache.doris.catalog.VariantType.class,
                replayed.getFullSchema().get(0).getType());
        Assertions.assertFalse(replayed.getFullSchema().get(0).getType()
                instanceof org.apache.doris.datasource.connector.converter.ConnectorComputeVariantType);
    }

    private static final class TestViewInfo extends BaseViewInfo {
        private TestViewInfo() {
            super(null, "select payload", Collections.emptyList());
        }

        private void createColumns(java.util.List<Slot> outputs) throws Exception {
            createFinalCols(outputs);
        }
    }
}
