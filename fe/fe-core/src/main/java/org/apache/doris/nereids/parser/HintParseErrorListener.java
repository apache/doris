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

package org.apache.doris.nereids.parser;

import org.apache.doris.nereids.exceptions.ParseException;

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CommonToken;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;

import java.util.Map;
import java.util.TreeMap;

/**
 * Hint listen parse error, and throw {@link ParseException} with reasonable message.
 */
public class HintParseErrorListener extends BaseErrorListener {

    private final Map<Integer, String> syntaxErrorMap = new TreeMap<>();
    private final String originSql;

    public HintParseErrorListener(String originSql) {
        this.originSql = originSql;
    }

    @Override
    public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine,
            String msg, RecognitionException e) {
        Origin start;
        if (offendingSymbol instanceof CommonToken) {
            CommonToken token = (CommonToken) offendingSymbol;
            start = new Origin(line, token.getCharPositionInLine());
        } else {
            start = new Origin(line, charPositionInLine);
        }
        StringBuilder sb = new StringBuilder();
        int startPosition = start.startPosition.get();
        sb.append("\n(line ").append(line).append(", pos ").append(startPosition).append(")").append("\n");
        sb.append(originSql);
        sb.append("\n");
        for (int i = 0; i < startPosition; i++) {
            sb.append("-");
        }
        sb.append("^^^\n");
        String err = sb.toString();
        syntaxErrorMap.put(charPositionInLine - 1, err);
    }

    public Map<Integer, String> getSyntaxErrorMap() {
        return syntaxErrorMap;
    }
}
