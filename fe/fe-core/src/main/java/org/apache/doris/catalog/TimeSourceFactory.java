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

import java.util.HashMap;
import java.util.Map;

public final class TimeSourceFactory {
    private static final Map<String, TimeSource> timeSourceMap = new HashMap<>();

    static {
        // register default time source
        TimeSourceFactory.register(new EmptyTimeSource());
        TimeSourceFactory.register(new UnixTimestampTimeSource());
    }

    public TimeSourceFactory() {
    }

    public static void register(TimeSource timeSource) {
        timeSourceMap.put(timeSource.getName(), timeSource);
    }

    // not check null && not check exists, guard by caller
    public static TimeSource get(String name) {
        return timeSourceMap.get(name);
    }

    // check timeSource by name exist in timeSourceMap
    public static boolean checkTimeSourceExist(String name) {
        return timeSourceMap.containsKey(name);
    }
}
