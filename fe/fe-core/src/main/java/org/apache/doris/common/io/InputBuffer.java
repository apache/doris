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

package org.apache.doris.common.io;

import java.io.ByteArrayInputStream;
import java.io.FilterInputStream;

/**
 * A reusable {@link InputStream} implementation that reads from an in-memory
 * buffer.
 *
 * <p>
 * This saves memory over creating a new InputStream and ByteArrayInputStream
 * each time data is read.
 *
 * <p>
 * Typical usage is something like the following:
 * 
 * <pre>
 *
 * InputBuffer buffer = new InputBuffer();
 * while (... loop condition ...) {
 *   byte[] data = ... get data ...;
 *   int dataLength = ... get data length ...;
 *   buffer.reset(data, dataLength);
 *   ... read buffer using InputStream methods ...
 * }
 * </pre>
 * 
 * @see DataInputBuffer
 * @see DataOutput
 */
public class InputBuffer extends FilterInputStream {

    private static class Buffer extends ByteArrayInputStream {
        public Buffer() {
            super(new byte[] {});
        }

        public void reset(byte[] input, int start, int length) {
            this.buf = input;
            this.count = start + length;
            this.mark = start;
            this.pos = start;
        }

        public int getPosition() {
            return pos;
        }

        public int getLength() {
            return count;
        }
    }

    private Buffer buffer;

    /** Constructs a new empty buffer. */
    public InputBuffer() {
        this(new Buffer());
    }

    private InputBuffer(Buffer buffer) {
        super(buffer);
        this.buffer = buffer;
    }

    /** Resets the data that the buffer reads. */
    public void reset(byte[] input, int length) {
        buffer.reset(input, 0, length);
    }

    /** Resets the data that the buffer reads. */
    public void reset(byte[] input, int start, int length) {
        buffer.reset(input, start, length);
    }

    /** Returns the current position in the input. */
    public int getPosition() {
        return buffer.getPosition();
    }

    /** Returns the length of the input. */
    public int getLength() {
        return buffer.getLength();
    }

}
