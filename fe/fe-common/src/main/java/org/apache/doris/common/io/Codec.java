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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Codec {

    // not support encode negative value now
    public static void encodeVarint64(long source, DataOutput out) throws IOException {
        assert source >= 0;
        short B = 128;

        while (source >= B) {
            out.write((int) (source & (B - 1) | B));
            source = source >> 7;
        }
        out.write((int) (source & (B - 1)));
    }

    // not support decode negative value now
    public static long decodeVarint64(DataInput in) throws IOException {
        long result = 0;
        int shift = 0;
        short B = 128;

        while (true) {
            int oneByte = in.readUnsignedByte();
            boolean isEnd = (oneByte & B) == 0;
            result = result | ((long) (oneByte & B - 1) << (shift * 7));
            if (isEnd) {
                break;
            }
            shift++;
        }

        return result;
    }
}


