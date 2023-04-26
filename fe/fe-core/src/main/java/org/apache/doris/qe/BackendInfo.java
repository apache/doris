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

package org.apache.doris.qe;

import java.util.HashMap;
import java.util.Map;

public class BackendInfo {

    // BackendInfo . Afterwards, memory infor may be added.
    private long coreSize = 1;

    private static Map<Long, BackendInfo> Info;
    private static long mincorSize = 9999;

    static {
        Info = new HashMap<>();
    }

    public BackendInfo(long coreSize) {
        this.coreSize = coreSize;
    }

    public long getcoreSize() {
        return coreSize;
    }

    public static void clear() {
        Info.clear();
        mincorSize = 9999;
    }

    public static void addBeInfo(long beId, long coreSize) {
        Info.put(beId, new BackendInfo(coreSize));
        mincorSize = Math.min(mincorSize, coreSize);
    }

    public static long getMincorSize() {
        return mincorSize;
    }

    public static long getparallelExecInstanceNum() {
        if (getMincorSize() == 9999) {
            return 1;
        }
        return (getMincorSize() + 1) / 2;
    }

    public static BackendInfo getBeInfoById(long beId) {
        return Info.get(beId);
    }
}
