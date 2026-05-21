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

package org.apache.doris.nereids.trees.expressions.functions.agg;

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.types.VariantType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class CountTest {
    @Test
    void testCountDistinctRejectsVariant() {
        Count count = new Count(true, SlotReference.of("v", VariantType.INSTANCE));

        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                count::checkLegalityAfterRewrite);
        Assertions.assertTrue(exception.getMessage().contains("COUNT DISTINCT does not support VARIANT argument"));
        Assertions.assertTrue(exception.getMessage().contains("Cast the VARIANT expression"));
    }

    @Test
    void testMultiDistinctCountRejectsVariant() {
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> new MultiDistinctCount(SlotReference.of("v", VariantType.INSTANCE)));
        Assertions.assertTrue(exception.getMessage().contains("COUNT DISTINCT does not support VARIANT argument"));
        Assertions.assertTrue(exception.getMessage().contains("Cast the VARIANT expression"));
    }
}
