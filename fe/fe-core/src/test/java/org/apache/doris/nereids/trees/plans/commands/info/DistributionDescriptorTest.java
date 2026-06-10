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

import org.apache.doris.analysis.HashDistributionDesc;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.DdlException;
import org.apache.doris.nereids.types.JsonType;
import org.apache.doris.nereids.types.VariantType;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class DistributionDescriptorTest {

    @Test
    public void testRejectJsonAndVariantTranslatedDistributionColumns() {
        Map<String, ColumnDefinition> columnMap = Maps.newHashMap();
        columnMap.put("json_col", new ColumnDefinition("json_col", JsonType.INSTANCE, false));
        columnMap.put("variant_col", new ColumnDefinition("variant_col", VariantType.INSTANCE, false));

        DistributionDescriptor jsonDesc = new DistributionDescriptor(
                true, false, 1, Lists.newArrayList("json_col"));
        jsonDesc.validate(columnMap, KeysType.DUP_KEYS);
        DdlException jsonException = Assertions.assertThrows(DdlException.class,
                () -> jsonDesc.translateToCatalogStyle()
                        .toDistributionInfo(Lists.newArrayList(new Column("json_col", Type.JSONB))));
        Assertions.assertEquals("JsonType type should not be used in distribution column[json_col].",
                jsonException.getDetailMessage());

        DistributionDescriptor variantDesc = new DistributionDescriptor(
                true, false, 1, Lists.newArrayList("variant_col"));
        variantDesc.validate(columnMap, KeysType.DUP_KEYS);
        DdlException variantException = Assertions.assertThrows(DdlException.class,
                () -> variantDesc.translateToCatalogStyle()
                        .toDistributionInfo(Lists.newArrayList(new Column("variant_col", Type.VARIANT))));
        Assertions.assertEquals("Variant type should not be used in distribution column[variant_col].",
                variantException.getDetailMessage());
    }

    @Test
    public void testRejectJsonAndVariantCatalogDistributionColumns() {
        Column jsonColumn = new Column("json_col", Type.JSONB);
        HashDistributionDesc jsonDesc = new HashDistributionDesc(1, Lists.newArrayList("json_col"));
        DdlException jsonException = Assertions.assertThrows(DdlException.class,
                () -> jsonDesc.toDistributionInfo(Lists.newArrayList(jsonColumn)));
        Assertions.assertEquals("JsonType type should not be used in distribution column[json_col].",
                jsonException.getDetailMessage());

        Column variantColumn = new Column("variant_col", Type.VARIANT);
        HashDistributionDesc variantDesc = new HashDistributionDesc(1, Lists.newArrayList("variant_col"));
        DdlException variantException = Assertions.assertThrows(DdlException.class,
                () -> variantDesc.toDistributionInfo(Lists.newArrayList(variantColumn)));
        Assertions.assertEquals("Variant type should not be used in distribution column[variant_col].",
                variantException.getDetailMessage());
    }
}
