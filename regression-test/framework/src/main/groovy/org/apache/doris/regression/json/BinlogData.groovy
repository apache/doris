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

package org.apache.doris.regression.json

class PartitionData {
    public Long partitionId
    public Long version
    public Long stid // sub txn id

    String toString() {
        return "(" + partitionId.toString() + ", " + version.toString() + ", " + stid + ")"
    }
}

class PartitionRecords {
    public List<PartitionData> partitionRecords

    Boolean contains(Long partitionId) {
        for (PartitionData data : partitionRecords) {
            if (data.partitionId == partitionId) {
                return true
            }
        }

        return false
    }

    String toString() {
        return partitionRecords.toString()
    }
}

class BinlogData {
    public Long commitSeq
    public Long txnId
    public String timeStamp
    public String label
    public Long dbId
    public Map<Long, PartitionRecords> tableRecords
    public List<Long> stids

    String toString() {
        return "(commitSeq: " + commitSeq
                    + ", txnId: " + txnId
                    + ", timestamp: " + timeStamp
                    + ", label: " + label
                    + ", dbId: " + dbId
                    +", subTxnIds: " + stids
                    + ", tableRecords: " + tableRecords
                    +")"
    }
}

