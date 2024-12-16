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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.plans.commands.info.ColumnDefinition;
import org.apache.doris.nereids.trees.plans.commands.info.IndexDefinition;
import org.apache.doris.nereids.types.VariantType;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class IndexDefinitionTest {
    @Test
    void testVariantIndexFormatV1() throws AnalysisException {
        IndexDefinition def = new IndexDefinition("variant_index", Lists.newArrayList("col1"), "INVERTED",
                                        null, "comment");
        try {
            boolean isIndexFormatV1 = true;
            def.checkColumn(new ColumnDefinition("col1", VariantType.INSTANCE, false, AggregateType.NONE, true,
                                    null, "comment"), KeysType.UNIQUE_KEYS, true, isIndexFormatV1);
            Assertions.fail("No exception throws.");
        } catch (AnalysisException e) {
            Assertions.assertTrue(e instanceof AnalysisException);
            Assertions.assertTrue(e.getMessage().contains("not supported in inverted index format V1"));
        }
    }
}
