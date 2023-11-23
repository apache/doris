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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.nereids.rules.rewrite.NormalizeToSlot.NormalizeToSlotContext;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.types.StringType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

public class NormalizeToSlotTest {

    @Test
    void testSlotReferenceWithItsAlias() {
        SlotReference slotReference = new SlotReference("c1", StringType.INSTANCE);
        Alias alias = new Alias(slotReference, "a1");
        Set<Alias> existsAliases = ImmutableSet.of(alias);
        List<Expression> sourceExpressions = ImmutableList.of(slotReference, alias);

        NormalizeToSlotContext context = NormalizeToSlotContext.buildContext(existsAliases, sourceExpressions);
        Assertions.assertEquals(slotReference, context.normalizeToUseSlotRef(slotReference));
        Assertions.assertEquals(alias.toSlot(), context.normalizeToUseSlotRef(alias));
        Assertions.assertEquals(Sets.newHashSet(sourceExpressions),
                context.pushDownToNamedExpression(sourceExpressions));
    }
}
