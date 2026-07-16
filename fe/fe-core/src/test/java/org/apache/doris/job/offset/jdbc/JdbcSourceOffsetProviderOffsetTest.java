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

import org.apache.doris.job.cdc.split.BinlogSplit;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;

public class JdbcSourceOffsetProviderOffsetTest {

    @Test
    public void testEndOffsetAdvancesWhenCurrentOffsetIsAhead() {
        assertEndOffsetAdvancesWhenCurrentOffsetIsAhead(new TestJdbcSourceOffsetProvider(-1));
    }

    @Test
    public void testTvfEndOffsetAdvancesWhenCurrentOffsetIsAhead() {
        assertEndOffsetAdvancesWhenCurrentOffsetIsAhead(new TestJdbcTvfSourceOffsetProvider(-1));
    }

    @Test
    public void testEndOffsetRemainsWhenItIsAheadOfCurrentOffset() {
        JdbcSourceOffsetProvider provider = new TestJdbcSourceOffsetProvider(1);
        Map<String, String> currentOffset = Collections.singletonMap("lsn", "100");
        Map<String, String> endOffset = Collections.singletonMap("lsn", "200");
        provider.setEndBinlogOffset(endOffset);
        provider.updateOffset(new JdbcOffset(
                Collections.singletonList(new BinlogSplit(currentOffset))));

        Assert.assertTrue(provider.hasMoreDataToConsume());
        Assert.assertEquals(endOffset, provider.getEndBinlogOffset());
    }

    @Test
    public void testStaleCompareDoesNotOverwriteRefreshedEndOffset() {
        JdbcSourceOffsetProvider provider = new RefreshingEndOffsetProvider();
        provider.setEndBinlogOffset(Collections.singletonMap("lsn", "100"));
        provider.updateOffset(new JdbcOffset(
                Collections.singletonList(new BinlogSplit(Collections.singletonMap("lsn", "200")))));

        Assert.assertTrue(provider.hasMoreDataToConsume());
        Assert.assertEquals(Collections.singletonMap("lsn", "300"), provider.getEndBinlogOffset());
    }

    private static void assertEndOffsetAdvancesWhenCurrentOffsetIsAhead(JdbcSourceOffsetProvider provider) {
        Map<String, String> staleEndOffset = Collections.singletonMap("lsn", "100");
        Map<String, String> committedOffset = Collections.singletonMap("lsn", "200");
        provider.setEndBinlogOffset(staleEndOffset);

        provider.updateOffset(new JdbcOffset(
                Collections.singletonList(new BinlogSplit(committedOffset))));

        Assert.assertEquals(staleEndOffset, provider.getEndBinlogOffset());
        Assert.assertFalse(provider.hasMoreDataToConsume());
        Assert.assertEquals(committedOffset, provider.getEndBinlogOffset());
        Assert.assertEquals("{\"lsn\":\"200\"}", provider.getShowMaxOffset());
    }

    private static class TestJdbcSourceOffsetProvider extends JdbcSourceOffsetProvider {
        private final int compareResult;

        TestJdbcSourceOffsetProvider(int compareResult) {
            this.compareResult = compareResult;
        }

        @Override
        protected int compareOffset(Map<String, String> offsetFirst, Map<String, String> offsetSecond) {
            return compareResult;
        }
    }

    private static class TestJdbcTvfSourceOffsetProvider extends JdbcTvfSourceOffsetProvider {
        private final int compareResult;

        TestJdbcTvfSourceOffsetProvider(int compareResult) {
            this.compareResult = compareResult;
        }

        @Override
        protected int compareOffset(Map<String, String> offsetFirst, Map<String, String> offsetSecond) {
            return compareResult;
        }
    }

    private static class RefreshingEndOffsetProvider extends JdbcSourceOffsetProvider {
        @Override
        protected int compareOffset(Map<String, String> offsetFirst, Map<String, String> offsetSecond) {
            synchronized (splitsLock) {
                endBinlogOffset = Collections.singletonMap("lsn", "300");
                hasMoreData = true;
            }
            return -1;
        }
    }
}
