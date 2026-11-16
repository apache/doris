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

package org.apache.doris.nereids.trees.expressions.functions.scalar;

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TokenizeTest extends TestWithFeService {

    @Test
    public void testTokenizeRejectsTrailingTokens() {
        Tokenize tokenize = new Tokenize(new StringLiteral("alpha beta"),
                new StringLiteral("\"parser\" = \"unicode\" trailing_tokens"));

        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                tokenize::checkLegalityBeforeTypeCoercion);
        Assertions.assertEquals("tokenize second argument must be properties format", exception.getMessage());
    }
}
