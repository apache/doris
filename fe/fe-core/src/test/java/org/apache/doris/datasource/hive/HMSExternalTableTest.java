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

package org.apache.doris.datasource.hive;

import mockit.Injectable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;


/**
 * Test class for HMSExternalTable, focusing on view-related functionality
 */
public class HMSExternalTableTest {
    private TestHMSExternalTable table;
    private static final String TEST_VIEW_TEXT = "SELECT * FROM test_table";
    private static final String TEST_EXPANDED_VIEW = "/* Presto View */";

    // Real example of a Presto View definition
    private static final String PRESTO_VIEW_ORIGINAL = "/* Presto View: eyJvcmlnaW5hbFNxbCI6IlNFTEVDVFxuICBkZXBhcnRtZW50XG4sIGxlbmd0aChkZXBhcnRtZW50KSBkZXBhcnRtZW50X2xlbmd0aFxuLCBkYXRlX3RydW5jKCd5ZWFyJywgaGlyZV9kYXRlKSB5ZWFyXG5GUk9NXG4gIGVtcGxveWVlc1xuIiwiY2F0YWxvZyI6ImhpdmUiLCJzY2hlbWEiOiJtbWNfaGl2ZSIsImNvbHVtbnMiOlt7Im5hbWUiOiJkZXBhcnRtZW50IiwidHlwZSI6InZhcmNoYXIifSx7Im5hbWUiOiJkZXBhcnRtZW50X2xlbmd0aCIsInR5cGUiOiJiaWdpbnQifSx7Im5hbWUiOiJ5ZWFyIiwidHlwZSI6ImRhdGUifV0sIm93bmVyIjoidHJpbm8vbWFzdGVyLTEtMS5jLTA1OTYxNzY2OThiZDRkMTcuY24tYmVpamluZy5lbXIuYWxpeXVuY3MuY29tIiwicnVuQXNJbnZva2VyIjpmYWxzZX0= */";

    // Expected SQL query after decoding and parsing
    private static final String EXPECTED_SQL = "SELECT\n  department\n, length(department) department_length\n, date_trunc('year', hire_date) year\nFROM\n  employees\n";

    @Injectable
    private HMSExternalCatalog mockCatalog;

    private HMSExternalDatabase mockDb;

    @BeforeEach
    public void setUp() {
        // Create a mock database with minimal required functionality
        mockDb = new HMSExternalDatabase(mockCatalog, 1L, "test_db", "remote_test_db") {
            @Override
            public String getFullName() {
                return "test_catalog.test_db";
            }
        };

        table = new TestHMSExternalTable(mockCatalog, mockDb);
    }

    @Test
    public void testGetViewText_Normal() {
        // Test regular view text retrieval
        table.setViewOriginalText(TEST_VIEW_TEXT);
        table.setViewExpandedText(TEST_VIEW_TEXT);
        Assertions.assertEquals(TEST_VIEW_TEXT, table.getViewText());
    }

    @Test
    public void testGetViewText_PrestoView() {
        // Test Presto view parsing including base64 decode and JSON extraction
        table.setViewOriginalText(PRESTO_VIEW_ORIGINAL);
        table.setViewExpandedText(TEST_EXPANDED_VIEW);
        Assertions.assertEquals(EXPECTED_SQL, table.getViewText());
    }

    @Test
    public void testGetViewText_InvalidPrestoView() {
        // Test handling of invalid Presto view definition
        String invalidPrestoView = "/* Presto View: invalid_base64_content */";
        table.setViewOriginalText(invalidPrestoView);
        table.setViewExpandedText(TEST_EXPANDED_VIEW);
        Assertions.assertEquals(invalidPrestoView, table.getViewText());
    }

    @Test
    public void testGetViewText_EmptyExpandedView() {
        // Test handling of empty expanded view text
        table.setViewOriginalText(TEST_VIEW_TEXT);
        table.setViewExpandedText("");
        Assertions.assertEquals(TEST_VIEW_TEXT, table.getViewText());
    }

    /**
     * Test implementation of HMSExternalTable that allows setting view texts
     * Uses parent's getViewText() implementation for actual testing
     */
    private static class TestHMSExternalTable extends HMSExternalTable {
        private String viewExpandedText;
        private String viewOriginalText;

        public TestHMSExternalTable(HMSExternalCatalog catalog, HMSExternalDatabase db) {
            super(1L, "test_table", "test_table", catalog, db);
        }

        @Override
        public String getViewExpandedText() {
            return viewExpandedText;
        }

        @Override
        public String getViewOriginalText() {
            return viewOriginalText;
        }

        public void setViewExpandedText(String viewExpandedText) {
            this.viewExpandedText = viewExpandedText;
        }

        public void setViewOriginalText(String viewOriginalText) {
            this.viewOriginalText = viewOriginalText;
        }

        @Override
        protected synchronized void makeSureInitialized() {
            this.objectCreated = true;
        }
    }
}
