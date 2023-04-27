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

import com.google.common.collect.Maps;

import java.util.Map;

public class MetaReplayState {

    public enum MetaState {
        UNKNOWN,
        OK,
        OUT_OF_DATE,
        REPLAY_EXCEPTION
    }

    public MetaState state;
    public long delayTime;
    public Throwable throwable;

    public MetaReplayState() {
        state = MetaState.UNKNOWN;
        delayTime = -1;
        throwable = null;
    }

    public synchronized void setOk() {
        state = MetaState.OK;
        delayTime = -1;
        throwable = null;
    }

    public synchronized void setException(Throwable throwable) {
        this.throwable = throwable;
        state = MetaState.REPLAY_EXCEPTION;
    }

    public synchronized void setOutOfDate(long currentTime, long synchronizedTime) {
        this.delayTime = currentTime - synchronizedTime;
        state = MetaState.OUT_OF_DATE;
    }

    public synchronized void setTransferToUnknown() {
        state = MetaState.UNKNOWN;
    }

    public synchronized Map<String, String> getInfo() {
        Map<String, String> resultMap = Maps.newHashMap();
        resultMap.put("state", state.name());
        resultMap.put("delay_time_s", String.valueOf(delayTime / 1000));
        resultMap.put("exception", throwable == null ? "NULL" : throwable.getMessage());
        return resultMap;
    }

}
