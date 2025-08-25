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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;

public class TextTest {
    static Stream<Arguments> provideTestData() {
        return Stream.of(
                Arguments.arguments("Hello".getBytes(StandardCharsets.UTF_8)),
                Arguments.arguments("helloé".getBytes(StandardCharsets.UTF_8)),
                Arguments.arguments(createBytes("中".getBytes(StandardCharsets.UTF_8), 1024 * 1024 * 16)),
                Arguments.arguments(createBytes("中".getBytes(StandardCharsets.UTF_8), 1024 * 1024 * 16 + 1)),
                Arguments.arguments(createBytes("中".getBytes(StandardCharsets.UTF_8), 1024 * 1024 * 32)),
                Arguments.arguments(createBytes("中".getBytes(StandardCharsets.UTF_8), 43214321)),
                Arguments.arguments("特殊\n\r\t字符".getBytes(StandardCharsets.UTF_8))
        );
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @MethodSource("provideTestData")
    void testReadString(byte[] inputBytes) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutput dataOutput = new DataOutputStream(bos);
        dataOutput.writeInt(inputBytes.length);
        dataOutput.write(inputBytes);
        String result = Text.readString(new DataInputStream(new ByteArrayInputStream(bos.toByteArray())));
        Assertions.assertEquals(new String(inputBytes, StandardCharsets.UTF_8), result);
    }

    private static byte[] createBytes(byte[] metaBytes, int scala) {
        byte[] bytes = new byte[metaBytes.length * scala];
        for (int i = 0; i < scala; i++) {
            System.arraycopy(metaBytes, 0, bytes, i * metaBytes.length, metaBytes.length);
        }
        return bytes;
    }
}
