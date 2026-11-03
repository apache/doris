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

package org.apache.doris.job.offset.s3;

import org.apache.doris.persist.gson.GsonUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class S3EventOffsetTest {
    @Test
    public void testSerializedOffsetPreservesKeysAndCountsUtf8Bytes() {
        List<String> files = Arrays.asList("logs/中文.csv", "logs/a\"b\\c.csv");
        S3EventOffset offset = new S3EventOffset(files);
        String expected = "{\"files\":[\"logs/中文.csv\",\"logs/a\\\"b\\\\c.csv\"]}";

        Assertions.assertEquals(expected, offset.toSerializedJson());
        Assertions.assertEquals(expected, offset.showRange());
        Assertions.assertEquals(expected.getBytes(StandardCharsets.UTF_8).length, offset.serializedSize());
        Assertions.assertEquals(files, GsonUtils.GSON.fromJson(expected, S3EventOffset.class).getFiles());
        Assertions.assertTrue(offset.isValidOffset());
    }

    @Test
    public void testTaskFilesCannotBeChangedThroughCallerList() {
        List<String> files = new ArrayList<>(Collections.singletonList("logs/a.csv"));
        S3EventOffset offset = new S3EventOffset(files);
        files.add("logs/b.csv");

        Assertions.assertEquals(Collections.singletonList("logs/a.csv"), offset.getFiles());
        Assertions.assertThrows(UnsupportedOperationException.class, () -> offset.getFiles().add("logs/c.csv"));
        Assertions.assertFalse(GsonUtils.GSON.fromJson("{}", S3EventOffset.class).isValidOffset());
        Assertions.assertFalse(new S3EventOffset(Collections.singletonList(" ")).isValidOffset());
    }
}
