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

package org.apache.doris.qe;

import org.apache.doris.analysis.SetType;
import org.apache.doris.analysis.SetVar;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.common.DdlException;
import org.apache.doris.thrift.TQueryOptions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class SessionVariableParquetWriterTest {
    private static final String WRITE_INDEX = "enable_parquet_write_page_index";
    private static final String PAGE_SIZE = "parquet_write_data_page_size";

    @Test
    void testDefaults() {
        TQueryOptions options = new SessionVariable().toThrift();
        Assertions.assertTrue(options.isEnableParquetWritePageIndex());
        Assertions.assertEquals(1048576, options.getParquetWriteDataPageSize());
    }

    @Test
    void testForwardingAndThriftSerialization() throws Exception {
        SessionVariable session = new SessionVariable();
        VariableMgr.setVar(session, new SetVar(SetType.SESSION, WRITE_INDEX, new StringLiteral("false")));
        VariableMgr.setVar(session, new SetVar(SetType.SESSION, PAGE_SIZE, new StringLiteral("262144")));
        SessionVariable forwarded = new SessionVariable();
        forwarded.setForwardedSessionVariables(session.getForwardVariables());
        TQueryOptions options = forwarded.toThrift();
        Assertions.assertTrue(options.isSetEnableParquetWritePageIndex());
        Assertions.assertFalse(options.isEnableParquetWritePageIndex());
        Assertions.assertTrue(options.isSetParquetWriteDataPageSize());
        Assertions.assertEquals(262144, options.getParquetWriteDataPageSize());
    }

    @ParameterizedTest
    @ValueSource(strings = {"1", "1048576", "2147483647"})
    void testValidPageSize(String value) throws Exception {
        SessionVariable session = new SessionVariable();
        VariableMgr.setVar(session, new SetVar(SetType.SESSION, PAGE_SIZE, new StringLiteral(value)));
        Assertions.assertEquals(Integer.parseInt(value), session.toThrift().getParquetWriteDataPageSize());
    }

    @ParameterizedTest
    @ValueSource(strings = {"0", "-1", "2147483648", "invalid"})
    void testInvalidPageSize(String value) {
        SessionVariable session = new SessionVariable();
        Assertions.assertThrows(DdlException.class, () -> VariableMgr.setVar(session,
                new SetVar(SetType.SESSION, PAGE_SIZE, new StringLiteral(value))));
        Assertions.assertEquals(1048576, session.toThrift().getParquetWriteDataPageSize());
    }
}
