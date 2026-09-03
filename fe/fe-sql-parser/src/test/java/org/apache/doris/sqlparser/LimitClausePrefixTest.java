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

package org.apache.doris.sqlparser;

import org.apache.doris.nereids.DorisParser;
import org.apache.doris.nereids.DorisParser.LimitClauseContext;
import org.apache.doris.nereids.parser.ParseErrorListener;
import org.apache.doris.nereids.parser.PostProcessor;

import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

class LimitClausePrefixTest {
    private final DorisSqlParser facade = new DorisSqlParser();

    @ParameterizedTest(name = "{0}")
    @MethodSource("limitClauses")
    void preservesLimitAndOffsetLabels(String sql, String expectedLimit, String expectedOffset) {
        DorisParser parser = new DorisParser(new CommonTokenStream(facade.newLexer(sql)));
        parser.addParseListener(new PostProcessor());
        parser.removeErrorListeners();
        parser.addErrorListener(new ParseErrorListener());
        parser.getInterpreter().setPredictionMode(PredictionMode.SLL);

        LimitClauseContext context = parser.limitClause();

        Assertions.assertEquals(Token.EOF, parser.getCurrentToken().getType());
        Assertions.assertEquals(expectedLimit, context.limit.getText());
        if (expectedOffset == null) {
            Assertions.assertNull(context.offset);
        } else {
            Assertions.assertEquals(expectedOffset, context.offset.getText());
        }
    }

    private static Stream<Arguments> limitClauses() {
        return Stream.of(
                Arguments.of("LIMIT 100", "100", null),
                Arguments.of("LIMIT 100 OFFSET 20", "100", "20"),
                Arguments.of("LIMIT 20, 100", "100", "20"));
    }
}
