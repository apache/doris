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

package org.apache.doris.common;

import org.apache.doris.qe.QueryState;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ErrorCodeMapperTest {

    @Test
    public void testMappingsInBatch() {
        Object[][] cases = new Object[][]{
                {"Query timeout", ErrorCode.ERR_QUERY_TIMEOUT},
                {"Timeout: query timeout", ErrorCode.ERR_QUERY_TIMEOUT},
                {"Timeout: Query timeout", ErrorCode.ERR_QUERY_TIMEOUT},
                {"Cross Region Read NonCR Blocks Failed", ErrorCode.ERR_CROSS_REGION_READ_NONCR_BLOCKS_FAILED},
                {"RemoteException: File does not exist", ErrorCode.ERR_REMOTE_FILE_NOT_EXIST},
                {"user: File does not exist", ErrorCode.ERR_REMOTE_FILE_NOT_EXIST},
                {"engine: Orc row reader nextBatch failed. user: File does not exist",
                        ErrorCode.ERR_REMOTE_FILE_NOT_EXIST},
                {"BlockMissingException: Could not obtain block", ErrorCode.ERR_READ_HDFS_FILE_FAILED_CN},
                {"IOException: some missing blocks", ErrorCode.ERR_READ_HDFS_FILE_FAILED_CN},
                {"IOException: Blocklist for abc has changed", ErrorCode.ERR_READ_HDFS_FILE_FAILED_CN},
                {"please check your sql", ErrorCode.ERR_SQL_PARSING_ERROR},
                {"table not found", ErrorCode.ERR_TABLE_DOES_NOT_EXIST_IN_DATABASE},
                {"org.apache.hadoop.hive.metastore.api.MetaException: failed to get table test_tbl",
                        ErrorCode.ERR_MetaException},
                {"org.apache.hadoop.hive.metastore.api.MetaException: Access denied for table test_tbl",
                        ErrorCode.ERR_TABLEACCESS_DENIED_ERROR},
                {"failed to get table db.tbl MetaException Permission denied",
                        ErrorCode.ERR_TABLEACCESS_DENIED_ERROR},
                {"get file split failed for table, err: MetaException Permission denied",
                        ErrorCode.ERR_TABLEACCESS_DENIED_ERROR},
                {"engine: Backend channel closed", ErrorCode.ERR_BACKEND_ERROR},
                {"engine: Backend connection dead", ErrorCode.ERR_BACKEND_OFFLINE},
                {"engine:Backend host offline", ErrorCode.ERR_BACKEND_OFFLINE},
                {"Backend Backend [id=1] encountered an error", ErrorCode.ERR_BACKEND_ERROR},
                {"Backend Backend replica offline", ErrorCode.ERR_BACKEND_OFFLINE},
                {"engine: No namenodes available under nameservice", ErrorCode.ERR_FAILED_TO_INIT_READER},
                {"get file split failed for table, err: No namenodes available under nameservice",
                        ErrorCode.ERR_FAILED_TO_INIT_READER},
                {"engine: Orc row reader nextBatch failed", ErrorCode.ERR_ORC_ROW_READER_NEXTBATCH_FAILED},
                {"engine: Orc row reader nextBatch failed. Read hdfs file failed",
                        ErrorCode.ERR_ORC_ROW_READER_NEXTBATCH_FAILED},
                {"engine: Orc row reader nextBatch failed. IOException: some missing blocks",
                        ErrorCode.ERR_ORC_ROW_READER_NEXTBATCH_FAILED},
                {"Memory: Allocator sys memory check failed", ErrorCode.ERR_CREATE_EXPR_MEM_ALLOC_FAILED},
                {"engine: Orc row reader nextBatch failed. Memory: Allocator sys memory check failed",
                        ErrorCode.ERR_CREATE_EXPR_MEM_ALLOC_FAILED},
                {"Memory: MEM_LIMIT_EXCEEDED", ErrorCode.ERR_PRECATCH_MEM_LIMIT_EXCEEDED},
                {"engine: Orc row reader nextBatch failed. Memory: MEM_LIMIT_EXCEEDED",
                        ErrorCode.ERR_PRECATCH_MEM_LIMIT_EXCEEDED},
                {"Function: date_trunc function second param only support argument is year|quarter|month|week|day|hour|minute|second",
                        ErrorCode.ERR_DATE_TRUNC_SECOND_PARAM_UNSUPPORTED},
                {"Function: date_trunc function second param only support argument is year|quarter|month|week|day|hour|minute|second, extra context",
                        ErrorCode.ERR_DATE_TRUNC_SECOND_PARAM_UNSUPPORTED},
        };

        for (Object[] c : cases) {
            String msg = (String) c[0];
            ErrorCode expected = (ErrorCode) c[1];
            ErrorCode actual = ErrorCodeMapper.getErrorCode(msg);
            Assertions.assertEquals(expected, actual, "mapping failed for: " + msg);
        }
    }

    @Test
    public void testQueryStateMapsUnknownError() {
        QueryState state = new QueryState();
        state.setError(ErrorCode.ERR_UNKNOWN_ERROR, "Query timeout");
        Assertions.assertEquals(ErrorCode.ERR_QUERY_TIMEOUT, state.getErrorCode());

        state.setError(ErrorCode.ERR_UNKNOWN_ERROR, null);
        Assertions.assertEquals(ErrorCode.ERR_UNKNOWN_ERROR, state.getErrorCode());

        state.setError(ErrorCode.ERR_BAD_DB_ERROR, "Query timeout");
        Assertions.assertEquals(ErrorCode.ERR_BAD_DB_ERROR, state.getErrorCode());
    }
}
