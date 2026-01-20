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

package org.apache.doris.planner;

import org.apache.doris.thrift.TJoinDistributionType;

import com.google.common.base.Preconditions;

/** DistributionMode */
public enum DistributionMode {
    NONE("NONE"),
    BROADCAST("BROADCAST"),
    PARTITIONED("PARTITIONED"),
    BUCKET_SHUFFLE("BUCKET_SHUFFLE");

    private final String description;

    DistributionMode(String descr) {
        this.description = descr;
    }

    @Override
    public String toString() {
        return description;
    }

    public TJoinDistributionType toThrift() {
        switch (this) {
            case NONE:
                return TJoinDistributionType.NONE;
            case BROADCAST:
                return TJoinDistributionType.BROADCAST;
            case PARTITIONED:
                return TJoinDistributionType.PARTITIONED;
            case BUCKET_SHUFFLE:
                return TJoinDistributionType.BUCKET_SHUFFLE;
            default:
                Preconditions.checkArgument(false, "Unknown DistributionMode: " + this);
        }
        return TJoinDistributionType.NONE;
    }
}
