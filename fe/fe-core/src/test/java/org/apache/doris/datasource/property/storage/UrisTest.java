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

package org.apache.doris.datasource.property.storage;

import org.apache.doris.common.AnalysisException;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class UrisTest {

    @Test
    public void testKeepsOrderAndCollapsesDuplicates() throws AnalysisException {
        List<String> uris = Uris.parse("[\"s3://bucket/b\", \"s3://bucket/a\", \"s3://bucket/b\"]");
        Assert.assertEquals(Arrays.asList("s3://bucket/b", "s3://bucket/a"), uris);
    }

    @Test
    public void testSingleElementIsOneLocation() throws AnalysisException {
        Assert.assertEquals(Arrays.asList("s3://bucket/a"), Uris.parse("[\"s3://bucket/a\"]"));
    }

    @Test
    public void testRejectsMissingOrBlankValue() {
        assertRejected(null);
        assertRejected("");
        assertRejected("   ");
    }

    @Test
    public void testRejectsMalformedJson() {
        assertRejected("s3://bucket/a");
        assertRejected("[");
        assertRejected("[\"s3://bucket/a\"");
    }

    @Test
    public void testRejectsNonArray() {
        assertRejected("\"s3://bucket/a\"");
        assertRejected("{\"uri\": \"s3://bucket/a\"}");
    }

    @Test
    public void testRejectsEmptyArray() {
        assertRejected("[]");
    }

    @Test
    public void testRejectsNonStringElement() {
        assertRejected("[\"s3://bucket/a\", 1]");
    }

    @Test
    public void testRejectsEmptyElement() {
        assertRejected("[\"s3://bucket/a\", \"\"]");
        assertRejected("[\"s3://bucket/a\", \"   \"]");
    }

    @Test
    public void testRejectsMoreThanMaxUris() {
        assertRejected(jsonArray(Uris.MAX_URIS + 1));
    }

    @Test
    public void testAcceptsExactlyMaxUris() throws AnalysisException {
        Assert.assertEquals(Uris.MAX_URIS, Uris.parse(jsonArray(Uris.MAX_URIS)).size());
    }

    private static String jsonArray(int size) {
        StringBuilder builder = new StringBuilder("[");
        for (int index = 0; index < size; index++) {
            if (index > 0) {
                builder.append(", ");
            }
            builder.append("\"s3://bucket/ds").append(index).append('"');
        }
        return builder.append(']').toString();
    }

    private static void assertRejected(String raw) {
        try {
            Uris.parse(raw);
            Assert.fail("'" + Uris.KEY + "' must be rejected: " + raw);
        } catch (AnalysisException expected) {
            Assert.assertTrue(expected.getMessage().contains(Uris.KEY));
        }
    }
}
