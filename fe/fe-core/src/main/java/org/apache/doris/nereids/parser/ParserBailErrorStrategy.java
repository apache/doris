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

import org.antlr.v4.runtime.InputMismatchException;
import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.misc.ParseCancellationException;

/**
 * Inspired by {@link org.antlr.v4.runtime.BailErrorStrategy}, which is used in two-stage parsing:
 * This error strategy allows the first stage of two-stage parsing to immediately terminate if an
 * error is encountered, and immediately fall back to the second stage. In addition to avoiding
 * wasted work by attempting to recover from errors here, the empty implementation of sync
 * improves the performance of the first stage.
 */
public class ParserBailErrorStrategy extends ParseErrorStrategy {
    /**
     * Instead of recovering from exception e, re-throw it wrapped in a
     * {@link ParseCancellationException} so it is not caught by the rule function catches. Use
     * {@link Exception#getCause} to get the original {@link RecognitionException}.
     */
    @Override
    public void recover(Parser recognizer, RecognitionException e) {
        ParserRuleContext context = recognizer.getContext();
        while (context != null) {
            context.exception = e;
            context = context.getParent();
        }
        throw new ParseCancellationException();
    }

    /**
     * Make sure we don't attempt to recover inline; if the parser successfully recovers, it won't
     * throw an exception.
     */
    @Override
    public Token recoverInline(Parser recognizer) throws RecognitionException {
        InputMismatchException e = new InputMismatchException(recognizer);
        ParserRuleContext context = recognizer.getContext();
        while (context != null) {
            context.exception = e;
            context = context.getParent();
        }
        throw new ParseCancellationException(e);
    }

    /** Make sure we don't attempt to recover from problems in subrules. */
    @Override
    public void sync(Parser recognizer) throws RecognitionException {

    }
}
