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

import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.thrift.TRuntimeFilterType;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Used for encoding and decoding of session variable runtime_filter_type
 */
public class RuntimeFilterTypeHelper {
    private static final Logger LOG = LogManager.getLogger(RuntimeFilterTypeHelper.class);

    public static final long ALLOWED_MASK = (TRuntimeFilterType.IN.getValue()
            | TRuntimeFilterType.BLOOM.getValue()
            | TRuntimeFilterType.MIN_MAX.getValue()
            | TRuntimeFilterType.IN_OR_BLOOM.getValue());

    private static final Map<String, Long> varValueSet = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);

    static {
        varValueSet.put("IN", (long) TRuntimeFilterType.IN.getValue());
        varValueSet.put("BLOOM_FILTER", (long) TRuntimeFilterType.BLOOM.getValue());
        varValueSet.put("MIN_MAX", (long) TRuntimeFilterType.MIN_MAX.getValue());
        varValueSet.put("IN_OR_BLOOM_FILTER", (long) TRuntimeFilterType.IN_OR_BLOOM.getValue());
    }

    // convert long type variable value to string type that user can read
    public static String decode(Long varValue) throws DdlException {
        // 0 parse to empty string
        if (varValue == 0) {
            return "";
        }
        if ((varValue & ~ALLOWED_MASK) != 0) {
            ErrorReport.reportDdlException(
                    ErrorCode.ERR_WRONG_VALUE_FOR_VAR, SessionVariable.RUNTIME_FILTER_TYPE, varValue);
        }

        List<String> names = new ArrayList<String>();
        for (Map.Entry<String, Long> value : getSupportedVarValue().entrySet()) {
            if ((varValue & value.getValue()) != 0) {
                names.add(value.getKey());
            }
        }

        return Joiner.on(',').join(names);
    }

    // convert string type variable value to long type that session can store
    public static Long encode(String varValue) throws DdlException {
        List<String> names = Splitter.on(',').trimResults().omitEmptyStrings().splitToList(varValue);

        // empty string parse to 0
        long resultCode = 0;
        for (String key : names) {
            long code = 0;
            if (StringUtils.isNumeric(key)) {
                code |= Long.parseLong(key);
            } else {
                code = getCodeFromString(key);
                if (code == 0) {
                    ErrorReport.reportDdlException(
                            ErrorCode.ERR_WRONG_VALUE_FOR_VAR, SessionVariable.RUNTIME_FILTER_TYPE, key);
                }
            }
            resultCode |= code;
            if ((resultCode & ~ALLOWED_MASK) != 0) {
                ErrorReport.reportDdlException(
                        ErrorCode.ERR_WRONG_VALUE_FOR_VAR, SessionVariable.RUNTIME_FILTER_TYPE, key);
            }
        }
        return resultCode;
    }

    // check if this variable value is supported
    public static boolean isSupportedVarValue(String varValue) {
        return varValue != null && getSupportedVarValue().containsKey(varValue);
    }

    // encode variable value from string to long
    private static long getCodeFromString(String varValue) {
        long code = 0;
        if (isSupportedVarValue(varValue)) {
            code |= getSupportedVarValue().get(varValue);
        }
        return code;
    }

    public static Map<String, Long> getSupportedVarValue() {
        return varValueSet;
    }
}
