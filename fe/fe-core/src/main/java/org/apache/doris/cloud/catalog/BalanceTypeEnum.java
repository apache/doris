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

package org.apache.doris.cloud.catalog;

import org.apache.doris.common.Config;

import lombok.Getter;

/**
 * Enum for balance type options
 */
@Getter
public enum BalanceTypeEnum {
    WITHOUT_WARMUP("without_warmup"),
    ASYNC_WARMUP("async_warmup"),
    SYNC_WARMUP("sync_warmup"),
    PEER_READ_ASYNC_WARMUP("peer_read_async_warmup");

    private final String value;

    BalanceTypeEnum(String value) {
        this.value = value;
    }

    /**
     * Parse string value to enum, case-insensitive
     */
    public static BalanceTypeEnum fromString(String value) {
        if (value == null) {
            return null;
        }
        for (BalanceTypeEnum type : BalanceTypeEnum.values()) {
            if (type.value.equalsIgnoreCase(value)) {
                return type;
            }
        }
        return null;
    }

    /**
     * Check if the given string is a valid balance type
     */
    public static boolean isValid(String value) {
        return fromString(value) != null;
    }

    /**
     * Get the balance type enum from the configuration string
     */
    public static BalanceTypeEnum getCloudWarmUpForRebalanceTypeEnum() {
        return fromString(Config.cloud_warm_up_for_rebalance_type) == null
            ? ComputeGroup.DEFAULT_COMPUTE_GROUP_BALANCE_ENUM : fromString(Config.cloud_warm_up_for_rebalance_type);
    }
}
