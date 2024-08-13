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

package org.apache.doris.transaction;

import org.apache.doris.catalog.Table;
import org.apache.doris.thrift.TSubTxnType;
import org.apache.doris.thrift.TTabletCommitInfo;

import lombok.Getter;

import java.util.List;

public class SubTransactionState {
    @Getter
    private long subTransactionId;
    @Getter
    private Table table;
    @Getter
    private List<TTabletCommitInfo> tabletCommitInfos;
    @Getter
    private SubTransactionType subTransactionType;

    public enum SubTransactionType {
        INSERT,
        DELETE
    }

    public SubTransactionState(long subTransactionId, Table table, List<TTabletCommitInfo> tabletCommitInfos,
            SubTransactionType subTransactionType) {
        this.subTransactionId = subTransactionId;
        this.table = table;
        this.tabletCommitInfos = tabletCommitInfos;
        this.subTransactionType = subTransactionType;
    }

    public static SubTransactionType getSubTransactionType(TSubTxnType subTxnType) {
        switch (subTxnType) {
            case INSERT:
                return SubTransactionType.INSERT;
            case DELETE:
                return SubTransactionType.DELETE;
            default:
                throw new IllegalArgumentException("Unknown sub txn type: " + subTxnType);
        }
    }

    public static TSubTxnType getSubTransactionType(SubTransactionType subTxnType) {
        switch (subTxnType) {
            case INSERT:
                return TSubTxnType.INSERT;
            case DELETE:
                return TSubTxnType.DELETE;
            default:
                throw new IllegalArgumentException("Unknown sub txn type: " + subTxnType);
        }
    }

    @Override
    public String toString() {
        return "SubTransactionState{"
                + "subTransactionId=" + subTransactionId
                + ", table=" + table
                + ", tabletCommitInfos=" + tabletCommitInfos
                + ", subTransactionType=" + subTransactionType
                + '}';
    }
}
