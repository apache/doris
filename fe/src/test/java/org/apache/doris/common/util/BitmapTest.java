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

package org.apache.doris.common.util;

import org.apache.doris.load.loadv2.Roaring64Map;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import static org.apache.doris.common.util.Util.decodeVarint64;
import static org.apache.doris.common.util.Util.encodeVarint64;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class BitmapTest {


    @Test
    public void testVarint64IntEncode() throws IOException {
        long[] sourceValue = {0, 1000, Integer.MAX_VALUE, Long.MAX_VALUE};
        for (long value : sourceValue) {
            ByteArrayOutputStream byteArrayOutput = new ByteArrayOutputStream();
            DataOutput output = new DataOutputStream(byteArrayOutput);
            encodeVarint64(value, output);
            assertEquals(value, decodeVarint64(new DataInputStream(new ByteArrayInputStream(byteArrayOutput.toByteArray()))));
        }
    }

    @Test
    public void testSerializeAndDeserializeInt64Bitmap() throws IOException {
        Roaring64Map roaring64Map = new Roaring64Map();
        for (long i = 0;i < 100; i++) {
            roaring64Map.add(i);
        }

        ByteArrayOutputStream byteArrayOutput = new ByteArrayOutputStream();
        DataOutput output = new DataOutputStream(byteArrayOutput);
        roaring64Map.serialize(output);

        DataInputStream inputStream = new DataInputStream(new ByteArrayInputStream(byteArrayOutput.toByteArray()));
        Roaring64Map deSerializedRoaring64Map = new Roaring64Map();
        deSerializedRoaring64Map.deserialize(inputStream);

        assertTrue(deSerializedRoaring64Map.equals(roaring64Map));
    }

}
