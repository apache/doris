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

package org.apache.doris.job.offset.jdbc;

import org.apache.doris.job.cdc.split.SnapshotSplit;
import org.apache.doris.job.exception.JobException;

import com.fasterxml.jackson.core.type.TypeReference;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

/**
 * Covers the remote-error handling added to {@link JdbcSourceOffsetProvider}:
 * the response-envelope parse must surface the original remote error instead of a
 * type-mismatch, and initOnCreate must fail fast when the remote reader cannot be opened.
 */
public class JdbcSourceOffsetProviderErrorHandlingTest {

    @Test
    public void testParseSuccessEnvelopeReturnsTypedPayload() throws JobException {
        JdbcSourceOffsetProvider provider = new JdbcSourceOffsetProvider();
        String response = "{\"code\":0,\"msg\":\"Success\","
                + "\"data\":[{\"splitId\":\"db.t:0\",\"tableId\":\"db.t\"}]}";
        List<SnapshotSplit> splits =
                provider.parseCdcResponseData(response, new TypeReference<List<SnapshotSplit>>() {});
        Assert.assertEquals(1, splits.size());
        Assert.assertEquals("db.t:0", splits.get(0).getSplitId());
        Assert.assertEquals("db.t", splits.get(0).getTableId());
    }

    @Test
    public void testParseFailureEnvelopeSurfacesOriginalError() {
        JdbcSourceOffsetProvider provider = new JdbcSourceOffsetProvider();
        String realError = "permission denied for database regression_db";
        String response = "{\"code\":1,\"msg\":\"Internal Error\",\"data\":\"" + realError + "\"}";
        try {
            provider.parseCdcResponseData(response, new TypeReference<List<SnapshotSplit>>() {});
            Assert.fail("a failed envelope must throw");
        } catch (JobException e) {
            Assert.assertTrue("the real remote error must be surfaced, got: " + e.getMessage(),
                    e.getMessage().contains(realError));
        }
    }

    @Test
    public void testParseUnparseableResponseThrows() {
        JdbcSourceOffsetProvider provider = new JdbcSourceOffsetProvider();
        String response = "<html>502 Bad Gateway</html>";
        try {
            provider.parseCdcResponseData(response, new TypeReference<Integer>() {});
            Assert.fail("an unparseable response must throw");
        } catch (JobException e) {
            Assert.assertTrue("the raw response must be surfaced, got: " + e.getMessage(),
                    e.getMessage().contains("502"));
        }
    }

    @Test
    public void testInitOnCreateFailsFastWhenReaderInitFails() {
        JdbcSourceOffsetProvider provider = new JdbcSourceOffsetProvider() {
            @Override
            protected void initSourceReader() throws JobException {
                throw new JobException("simulated reader init failure");
            }
        };
        try {
            provider.initOnCreate(Collections.singletonList("db.t"));
            Assert.fail("CREATE JOB must fail when the remote reader cannot be opened");
        } catch (JobException e) {
            Assert.assertTrue(e.getMessage().contains("simulated reader init failure"));
        }
    }
}
