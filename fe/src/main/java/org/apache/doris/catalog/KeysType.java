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

package org.apache.doris.catalog;

import org.apache.doris.thrift.TKeysType;

public enum KeysType {
    PRIMARY_KEYS,
    DUP_KEYS,
    UNIQUE_KEYS,
    AGG_KEYS;

    public boolean isAggregationFamily() {
        switch (this) {
            case AGG_KEYS:
            case UNIQUE_KEYS:
                return true;
            default:
                return false;
        }
    }

    public TKeysType toThrift() {
        switch (this) {
            case PRIMARY_KEYS:
                return TKeysType.PRIMARY_KEYS;
            case DUP_KEYS:
                return TKeysType.DUP_KEYS;
            case UNIQUE_KEYS:
                return TKeysType.UNIQUE_KEYS;
            case AGG_KEYS:
                return TKeysType.AGG_KEYS;
            default:
                return null;
        }
    }

    public String toSql() {
        switch (this) {
            case PRIMARY_KEYS:
                return "PRIMARY KEY";
            case DUP_KEYS:
                return "DUPLICATE KEY";
            case UNIQUE_KEYS:
                return "UNIQUE KEY";
            case AGG_KEYS:
                return "AGGREGATE KEY";
            default:
                return null;
        }
    }
}

