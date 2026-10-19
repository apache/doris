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

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.misc.Interval;

import java.util.Optional;
import java.util.function.Supplier;

/**
 * Utils for parser.
 */
public class ParserUtils {
    private static final ThreadLocal<Origin> threadLocal = new ThreadLocal<>();

    /** getOrigin */
    public static Optional<Origin> getOrigin() {
        Thread thread = Thread.currentThread();
        Origin origin;
        if (thread instanceof OriginAware) {
            // fast path
            origin = ((OriginAware) thread).getOrigin();
        } else {
            // slow path
            origin = threadLocal.get();
        }
        return Optional.ofNullable(origin);
    }

    /** withOrigin */
    public static <T> T withOrigin(ParserRuleContext ctx, Supplier<T> f) {
        Token startToken = ctx.getStart();
        Origin origin = new Origin(
                Optional.of(startToken.getLine()),
                Optional.of(startToken.getCharPositionInLine())
        );

        Thread thread = Thread.currentThread();
        if (thread instanceof OriginAware) {
            // fast path
            OriginAware aware = (OriginAware) thread;
            Origin outerOrigin = aware.getOrigin();
            try {
                aware.setOrigin(origin);
                return f.get();
            } finally {
                aware.setOrigin(outerOrigin);
            }
        } else {
            // slow path
            Origin outerOrigin = threadLocal.get();
            try {
                threadLocal.set(origin);
                return f.get();
            } finally {
                if (outerOrigin != null) {
                    threadLocal.set(outerOrigin);
                } else {
                    threadLocal.remove();
                }
            }
        }
    }

    public static String command(ParserRuleContext ctx) {
        CharStream stream = ctx.getStart().getInputStream();
        return stream.getText(Interval.of(0, stream.size() - 1));
    }

    public static Origin position(Token token) {
        return new Origin(token.getLine(), token.getCharPositionInLine());
    }

    /**
     * getSecond
     */
    public static long getSecond(long value, String s) {
        switch (s) {
            case "DAY":
                return value * 24 * 60 * 60;
            case "HOUR":
                return value * 60 * 60;
            default:
                return value;
        }
    }
}
