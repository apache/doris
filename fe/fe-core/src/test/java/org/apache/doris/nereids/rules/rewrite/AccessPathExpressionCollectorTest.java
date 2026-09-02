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

import org.apache.doris.analysis.ColumnAccessPathType;
import org.apache.doris.nereids.rules.rewrite.AccessPathExpressionCollector.CollectAccessPathResult;
import org.apache.doris.nereids.rules.rewrite.NestedColumnPruning.DataTypeAccessTree;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ElementAt;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.StructField;
import org.apache.doris.nereids.types.StructType;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public class AccessPathExpressionCollectorTest {

    @Test
    public void testStructAccessPathUsesRootLocale() {
        Locale originalLocale = Locale.getDefault();
        try {
            Locale.setDefault(Locale.forLanguageTag("tr-TR"));
            StructType structType = new StructType(ImmutableList.of(
                    new StructField("I", IntegerType.INSTANCE, true, ""),
                    new StructField("ı", StringType.INSTANCE, true, "")));
            SlotReference slot = new SlotReference("I", structType);
            Multimap<Integer, CollectAccessPathResult> accessPaths = ArrayListMultimap.create();
            AccessPathExpressionCollector collector =
                    new AccessPathExpressionCollector(null, accessPaths, false, false);

            collector.collect(new ElementAt(slot, new StringLiteral("I")));

            List<CollectAccessPathResult> results = new ArrayList<>(
                    accessPaths.get(slot.getExprId().asInt()));
            Assertions.assertEquals(1, results.size());
            Assertions.assertEquals(ImmutableList.of("i", "i"), results.get(0).getPath());

            DataTypeAccessTree tree = DataTypeAccessTree.ofRoot(slot, ColumnAccessPathType.DATA);
            tree.setAccessByPath(results.get(0).getPath(), 0, ColumnAccessPathType.DATA);
            StructType prunedType = (StructType) tree.pruneDataType().orElseThrow();
            Assertions.assertEquals(1, prunedType.getFields().size());
            Assertions.assertEquals("i", prunedType.getFields().get(0).getName());
            Assertions.assertEquals(IntegerType.INSTANCE, prunedType.getFields().get(0).getDataType());
        } finally {
            Locale.setDefault(originalLocale);
        }
    }
}
