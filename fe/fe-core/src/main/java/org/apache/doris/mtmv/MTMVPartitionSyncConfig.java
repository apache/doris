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

package org.apache.doris.mtmv;

import java.util.Optional;

public class MTMVPartitionSyncConfig {
    private int syncLimit;
    private MTMVPartitionSyncTimeUnit timeUnit;
    private Optional<String> dateFormat;

    public MTMVPartitionSyncConfig(int syncLimit, MTMVPartitionSyncTimeUnit timeUnit,
            Optional<String> dateFormat) {
        this.syncLimit = syncLimit;
        this.timeUnit = timeUnit;
        this.dateFormat = dateFormat;
    }

    public int getSyncLimit() {
        return syncLimit;
    }

    public void setSyncLimit(int syncLimit) {
        this.syncLimit = syncLimit;
    }

    public MTMVPartitionSyncTimeUnit getTimeUnit() {
        return timeUnit;
    }

    public void setTimeUnit(MTMVPartitionSyncTimeUnit timeUnit) {
        this.timeUnit = timeUnit;
    }

    public Optional<String> getDateFormat() {
        return dateFormat;
    }

    public void setDateFormat(Optional<String> dateFormat) {
        this.dateFormat = dateFormat;
    }
}
