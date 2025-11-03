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

package org.apache.doris.job.extensions.mtmv;

import org.apache.doris.job.extensions.mtmv.MTMVTask.MTMVTaskTriggerMode;

import com.google.gson.annotations.SerializedName;

import java.util.List;

public class MTMVTaskContext {

    @SerializedName(value = "triggerMode")
    private MTMVTaskTriggerMode triggerMode;

    @SerializedName(value = "partitions")
    private List<String> partitions;

    @SerializedName(value = "isComplete")
    private boolean isComplete;

    public MTMVTaskContext(MTMVTaskTriggerMode triggerMode) {
        this.triggerMode = triggerMode;
    }

    public MTMVTaskContext(MTMVTaskTriggerMode triggerMode, List<String> partitions, boolean isComplete) {
        this.triggerMode = triggerMode;
        this.partitions = partitions;
        this.isComplete = isComplete;
    }

    public List<String> getPartitions() {
        return partitions;
    }

    public MTMVTaskTriggerMode getTriggerMode() {
        return triggerMode;
    }

    public boolean isComplete() {
        return isComplete;
    }

    @Override
    public String toString() {
        return "MTMVTaskContext{"
                + "triggerMode=" + triggerMode
                + ", partitions=" + partitions
                + ", isComplete=" + isComplete
                + '}';
    }
}
