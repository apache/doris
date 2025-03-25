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
// This file is copied from
// https://github.com/trinodb/trino/blob/438/plugin/trino-hive/src/main/java/io/trino/plugin/hive/fs/TransactionDirectoryListingCacheKey.java
// and modified by Doris

package org.apache.doris.fs;

import java.util.Objects;

public class TransactionDirectoryListingCacheKey {

    private final long transactionId;
    private final String path;

    public TransactionDirectoryListingCacheKey(long transactionId, String path) {
        this.transactionId = transactionId;
        this.path = Objects.requireNonNull(path, "path is null");
    }

    public String getPath() {
        return path;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TransactionDirectoryListingCacheKey that = (TransactionDirectoryListingCacheKey) o;
        return transactionId == that.transactionId && path.equals(that.path);
    }

    @Override
    public int hashCode() {
        return Objects.hash(transactionId, path);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("TransactionDirectoryListingCacheKey{");
        sb.append("transactionId=").append(transactionId);
        sb.append(", path='").append(path).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
