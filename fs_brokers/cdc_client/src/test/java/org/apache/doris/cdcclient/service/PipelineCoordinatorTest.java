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

package org.apache.doris.cdcclient.service;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.HexFormat;
import java.util.List;

class PipelineCoordinatorTest {

    private static final String RECORD = "挖隧道、修道路";
    private static final String EXPECTED_UTF8_HEX =
            "e68c96e99aa7e98193e38081e4bfaee98193e8b7af";

    @Test
    void encodeRecordUsesUtf8WhenDefaultCharsetIsNotUtf8() throws Exception {
        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        String classpath =
                System.getProperty(
                        "surefire.test.class.path", System.getProperty("java.class.path"));
        Process process =
                new ProcessBuilder(
                                java,
                                "-Dfile.encoding=US-ASCII",
                                "-cp",
                                classpath,
                                EncodingProbe.class.getName())
                        .redirectErrorStream(true)
                        .start();

        List<String> output;
        try (BufferedReader reader =
                new BufferedReader(
                        new InputStreamReader(
                                process.getInputStream(), StandardCharsets.US_ASCII))) {
            output = reader.lines().toList();
        }

        assertEquals(0, process.waitFor(), String.join(System.lineSeparator(), output));
        assertEquals(List.of("US-ASCII", EXPECTED_UTF8_HEX), output);
    }

    public static final class EncodingProbe {

        public static void main(String[] args) {
            System.out.println(Charset.defaultCharset().name());
            System.out.println(HexFormat.of().formatHex(PipelineCoordinator.encodeRecord(RECORD)));
        }
    }
}
