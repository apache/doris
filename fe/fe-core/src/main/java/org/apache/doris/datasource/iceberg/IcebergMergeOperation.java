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

package org.apache.doris.datasource.iceberg;

/**
 * Operation codes used for merge-style DML routing.
 */
public final class IcebergMergeOperation {
    public static final String OPERATION_COLUMN = "operation";

    // Merge sink routing:
    // 1 (INSERT): only insert writer
    // 2 (DELETE): only delete writer
    // 3 (UPDATE): update rows (merge sink writes delete + insert)
    // 4 (UPDATE_INSERT): pre-split update insert rows
    // 5 (UPDATE_DELETE): pre-split update delete rows
    public static final byte INSERT_OPERATION_NUMBER = 1;
    public static final byte DELETE_OPERATION_NUMBER = 2;
    public static final byte UPDATE_OPERATION_NUMBER = 3;
    public static final byte UPDATE_INSERT_OPERATION_NUMBER = 4;
    public static final byte UPDATE_DELETE_OPERATION_NUMBER = 5;

    private IcebergMergeOperation() {}
}
