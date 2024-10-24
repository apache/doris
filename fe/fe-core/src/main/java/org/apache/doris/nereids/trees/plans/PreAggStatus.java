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

package org.apache.doris.nereids.trees.plans;

import java.util.function.Supplier;

/**
 * Pre-aggregate status for OLAP scan table.
 * @see org.apache.doris.planner.OlapScanNode#isPreAggregation
 */
public class PreAggStatus {

    private enum Status {
        ON, OFF, UNSET
    }

    private static final PreAggStatus PRE_AGG_ON = new PreAggStatus(Status.ON, "");
    private static final PreAggStatus PRE_AGG_UNSET = new PreAggStatus(Status.UNSET, "");
    private final Status status;
    private final String offReason;

    private PreAggStatus(Status status, String offReason) {
        this.status = status;
        this.offReason = offReason;
    }

    public boolean isOn() {
        return status == Status.ON;
    }

    public boolean isOff() {
        return status == Status.OFF;
    }

    public boolean isUnset() {
        return status == Status.UNSET;
    }

    public String getOffReason() {
        return offReason;
    }

    public PreAggStatus offOrElse(Supplier<PreAggStatus> supplier) {
        if (status == Status.OFF) {
            return this;
        } else {
            return supplier.get();
        }
    }

    public static PreAggStatus unset() {
        return PRE_AGG_UNSET;
    }

    public static PreAggStatus on() {
        return PRE_AGG_ON;
    }

    public static PreAggStatus off(String reason) {
        return new PreAggStatus(Status.OFF, reason);
    }

    @Override
    public String toString() {
        if (status == Status.ON) {
            return "ON";
        } else if (status == Status.OFF) {
            return "OFF, " + offReason;
        } else {
            return "UNSET";
        }
    }
}
