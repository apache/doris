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

import com.google.common.collect.ImmutableMap;
import org.antlr.v4.runtime.DefaultErrorStrategy;
import org.antlr.v4.runtime.InputMismatchException;
import org.antlr.v4.runtime.NoViableAltException;
import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Token;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;

/**
 * A ParserErrorStrategy extends the {@link DefaultErrorStrategy}, that does special handling
 * on errors.
 *
 * The intention of this class is to provide more information of these errors encountered in ANTLR
 * parser to the downstream consumers.
 */
public class ParseErrorStrategy extends DefaultErrorStrategy {
    private static final Map<String, String> USER_WORD_DICT = ImmutableMap.of("'<EOF>'", "end of input");

    @Override
    protected String getTokenErrorDisplay(Token t) {
        String tokenName = super.getTokenErrorDisplay(t);
        return USER_WORD_DICT.getOrDefault(tokenName, tokenName);
    }

    @Override
    protected void reportInputMismatch(Parser recognizer, InputMismatchException e) {
        recognizer.notifyErrorListeners(e.getOffendingToken(),
                constructErrorMsg(e.getOffendingToken(), ""), e);
    }

    @Override
    protected void reportNoViableAlternative(Parser recognizer, NoViableAltException e) {
        recognizer.notifyErrorListeners(e.getOffendingToken(),
                constructErrorMsg(e.getOffendingToken(), ""), e);
    }

    @Override
    protected void reportUnwantedToken(Parser recognizer) {
        if (this.inErrorRecoveryMode(recognizer)) {
            return;
        }
        this.beginErrorCondition(recognizer);
        recognizer.notifyErrorListeners(recognizer.getCurrentToken(),
                constructErrorMsg(recognizer.getCurrentToken(),
                        "extra input " + getTokenErrorDisplay(recognizer.getCurrentToken())),
                new RecognitionException(null, null, null));
    }

    @Override
    protected void reportMissingToken(Parser recognizer) {
        if (this.inErrorRecoveryMode(recognizer)) {
            return;
        }
        this.beginErrorCondition(recognizer);
        recognizer.notifyErrorListeners(recognizer.getCurrentToken(),
                constructErrorMsg(recognizer.getCurrentToken(),
                        "missing " + getExpectedTokens(recognizer).toString(recognizer.getVocabulary())),
                new RecognitionException(null, null, null));

    }

    private String constructErrorMsg(Token token, String extraMessage) {
        StringBuilder messageBuilder = new StringBuilder();
        messageBuilder.append("Syntax error at or near ");
        messageBuilder.append(getTokenErrorDisplay(token));
        if (StringUtils.isNotEmpty(extraMessage)) {
            messageBuilder.append(": ");
            messageBuilder.append(extraMessage);
        }
        return messageBuilder.toString();
    }
}
