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
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.IntStream;
import org.antlr.v4.runtime.misc.Interval;

/**
 * Translate parser stream to insensitive.
 */
public class CaseInsensitiveStream implements CharStream {
    private final CharStream stream;

    public CaseInsensitiveStream(CharStream stream) {
        this.stream = stream;
    }

    /**
     * Avoid copying strings whose UTF-16 indices already equal code-point indices.
     * Strings containing surrogates keep ANTLR's code-point stream and indexing semantics.
     */
    public static CharStream fromString(String input) {
        for (int index = 0; index < input.length(); index++) {
            if (Character.isSurrogate(input.charAt(index))) {
                return new CaseInsensitiveStream(CharStreams.fromString(input));
            }
        }
        return new CaseInsensitiveStringStream(input);
    }

    @Override
    public String getText(Interval interval) {
        return stream.getText(interval);
    }

    @Override
    public void consume() {
        stream.consume();
    }

    @Override
    public int LA(int i) {
        return toUpperCase(stream.LA(i));
    }

    private static int toUpperCase(int result) {
        if (result >= 'a' && result <= 'z') {
            return result - ('a' - 'A');
        }
        switch (result) {
            case 0:
            case IntStream.EOF:
                return result;
            default:
                return Character.toUpperCase(result);
        }
    }

    @Override
    public int mark() {
        return stream.mark();
    }

    @Override
    public void release(int marker) {
        stream.release(marker);
    }

    @Override
    public int index() {
        return stream.index();
    }

    @Override
    public void seek(int index) {
        stream.seek(index);
    }

    @Override
    public int size() {
        return stream.size();
    }

    @Override
    public String getSourceName() {
        return stream.getSourceName();
    }

    private static final class CaseInsensitiveStringStream implements CharStream {
        private final String input;
        private int position;

        CaseInsensitiveStringStream(String input) {
            this.input = input;
        }

        @Override
        public String getText(Interval interval) {
            int start = Math.min(interval.a, input.length());
            int length = Math.min(interval.b - interval.a + 1, input.length() - start);
            return input.substring(start, start + length);
        }

        @Override
        public void consume() {
            if (position == input.length()) {
                throw new IllegalStateException("cannot consume EOF");
            }
            position++;
        }

        @Override
        public int LA(int offset) {
            int index;
            switch (Integer.signum(offset)) {
                case -1:
                    index = position + offset;
                    return index < 0 ? IntStream.EOF : toUpperCase(input.charAt(index));
                case 0:
                    return 0;
                case 1:
                    index = position + offset - 1;
                    return index >= input.length() ? IntStream.EOF : toUpperCase(input.charAt(index));
                default:
                    throw new UnsupportedOperationException("Not reached");
            }
        }

        @Override
        public int mark() {
            return -1;
        }

        @Override
        public void release(int marker) {
        }

        @Override
        public int index() {
            return position;
        }

        @Override
        public void seek(int index) {
            position = index;
        }

        @Override
        public int size() {
            return input.length();
        }

        @Override
        public String getSourceName() {
            return IntStream.UNKNOWN_SOURCE_NAME;
        }
    }
}
