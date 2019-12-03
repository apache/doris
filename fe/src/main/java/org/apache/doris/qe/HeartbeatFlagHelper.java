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

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Maps;
import org.apache.doris.common.AnalysisException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class HeartbeatFlagHelper {

    /* Bits for different Heartbeat Flag modes, you can add custom flags here */
    public static final long FLAG_SET_DEFAULT_ROWSET_TYPE_TO_BETA = 1L;

    /* default control flag */
    public final static long DEFAULT_HEARTBEAT_FLAGS = 0L;

    public final static long FLAG_ALLOWED_MASK = (FLAG_SET_DEFAULT_ROWSET_TYPE_TO_BETA);

    private final static Map<String, Long> validFlags = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);

    private final static Map<String, Long> combineFlagsSet = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);

    static {
        validFlags.put("SET_DEFAULT_ROWSET_TYPE_TO_BETA", FLAG_SET_DEFAULT_ROWSET_TYPE_TO_BETA);
    }

    // convert long type flags to string type that user can read
    public static String analyze(Long flags) throws AnalysisException {
        // 0 parse to empty string
        if (flags == 0) {
            return "";
        }
        if ((flags & ~FLAG_ALLOWED_MASK) != 0) {
            throw new AnalysisException("Variable 'heartbeat_flags' can't be set to the value of: " + flags);
        }

        List<String> names = new ArrayList<String>();
        for (Map.Entry<String, Long> mode : getSupportControlFlags().entrySet()) {
            if ((flags & mode.getValue()) != 0) {
                names.add(mode.getKey());
            }
        }

        return Joiner.on(',').join(names);
    }

    // convert string type heartbeat flags to long type
    public static Long analyze(String flags) throws AnalysisException {
        List<String> names =
                Splitter.on(',').trimResults().omitEmptyStrings().splitToList(flags);

        // empty string parse to 0
        long value = 0L;
        for (String key : names) {
            // the SQL MODE must be supported, set sql mode repeatedly is not allowed
            if (!isSupportedFlag(key) || (value & getSupportControlFlags().get(key)) != 0) {
                throw new AnalysisException("Variable 'sql_mode' can't be set to the value of: " + key);
            }
            value |= getSupportControlFlags().get(key);
        }

        return value;
    }

    // check if this control flag is supported
    public static boolean isSupportedFlag(String flag) {
        // empty string is valid and equals to 0L
        if (flag == null || !getSupportControlFlags().containsKey(flag)) {
            return false;
        }
        return true;
    }

    // check if this SQL MODE is combine mode
    public static boolean isCombineFlag(String key) {
        return combineFlagsSet.containsKey(key);
    }

    public static Map<String, Long> getSupportControlFlags() {
        return validFlags;
    }

    public static Map<String, Long> getCombineMode() {
        return combineFlagsSet;
    }

}
