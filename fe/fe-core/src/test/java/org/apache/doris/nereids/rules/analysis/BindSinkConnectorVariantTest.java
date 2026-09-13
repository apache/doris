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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.common.Config;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.types.ConnectorComputeVariantType;
import org.apache.doris.nereids.types.JsonType;
import org.apache.doris.nereids.types.VariantType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class BindSinkConnectorVariantTest {

    @Test
    void defaultOffCtasAndMtmvUseJsonCarrierForRootConnectorVariant() {
        boolean originalEnableVariantV2 = Config.enable_variant_v2;
        try {
            Config.enable_variant_v2 = false;
            Expression source = SlotReference.of("payload", ConnectorComputeVariantType.INSTANCE);

            Expression coerced = BindSink.coerceSinkExpression(source, VariantType.INSTANCE);

            Cast targetCast = Assertions.assertInstanceOf(Cast.class, coerced);
            Assertions.assertEquals(VariantType.INSTANCE, targetCast.getDataType());
            Cast carrierCast = Assertions.assertInstanceOf(Cast.class, targetCast.child());
            Assertions.assertEquals(JsonType.INSTANCE, carrierCast.getDataType());
            Assertions.assertSame(source, carrierCast.child());
        } finally {
            Config.enable_variant_v2 = originalEnableVariantV2;
        }
    }

    @Test
    void variantV2TargetKeepsDirectCarrier() {
        boolean originalEnableVariantV2 = Config.enable_variant_v2;
        try {
            Config.enable_variant_v2 = true;
            Expression source = SlotReference.of("payload", ConnectorComputeVariantType.INSTANCE);

            Cast coerced = Assertions.assertInstanceOf(Cast.class,
                    BindSink.coerceSinkExpression(source, VariantType.INSTANCE));

            Assertions.assertEquals(VariantType.INSTANCE, coerced.getDataType());
            Assertions.assertSame(source, coerced.child());
        } finally {
            Config.enable_variant_v2 = originalEnableVariantV2;
        }
    }
}
