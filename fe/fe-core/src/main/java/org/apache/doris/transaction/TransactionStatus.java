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

public enum TransactionStatus {
    UNKNOWN(0),
    PREPARE(1),
    COMMITTED(2),
    VISIBLE(3),
    ABORTED(4),
    PRECOMMITTED(5);
    
    private final int flag;
    
    private TransactionStatus(int flag) {
        this.flag = flag;
    }
    
    public int value() {
        return flag;
    }
    
    public static TransactionStatus valueOf(int flag) {
        switch (flag) {
            case 0:
                return UNKNOWN;
            case 1:
                return PREPARE;
            case 2:
                return COMMITTED;
            case 3:
                return VISIBLE;
            case 4:
                return ABORTED;
            case 5:
                return PRECOMMITTED;
            default:
                return null;
        }
    }
    
    public boolean isFinalStatus() {
        return this == TransactionStatus.VISIBLE || this == TransactionStatus.ABORTED;
    }

    @Override
    public String toString() {
        switch (this) {
            case UNKNOWN:
                return "UNKNOWN";
            case PREPARE:
                return "PREPARE";
            case COMMITTED:
                return "COMMITTED";
            case VISIBLE:
                return "VISIBLE";
            case ABORTED:
                return "ABORTED";
            case PRECOMMITTED:
                return "PRECOMMITTED";
            default:
                return "UNKNOWN";
        }
    }
}
