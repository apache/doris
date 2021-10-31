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

import org.apache.commons.lang3.StringUtils;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;

/**
 * Used for encoding and decoding of session variable exec_scheduling_strategy
 */
public class ExecSchedulingStrategyTypeHelper {

  // convert long type variable value to string type that user can read
  public static String decode(Long varValue) throws DdlException {
    String ret = null;
    for (ExecSchedulingStrategy strategy : ExecSchedulingStrategy.values()) {
      if (strategy.getValue() == varValue) {
        ret = strategy.name();
        break;
      }
    }
    if (StringUtils.isEmpty(ret)) {
      ErrorReport.reportDdlException(ErrorCode.ERR_WRONG_VALUE_FOR_VAR, SessionVariable.EXEC_SCHEDULING_STRATEGY, varValue);
    }
    return ret;
  }

  // convert string type variable value to long type that session can store
  public static long encode(String varValue) throws DdlException {
    if (StringUtils.isNumeric(varValue)) {
      return Long.parseLong(varValue);
    }

    Long ret = null;
    for (ExecSchedulingStrategy strategy : ExecSchedulingStrategy.values()) {
      if (strategy.name().equalsIgnoreCase(varValue)) {
        ret = strategy.getValue();
      }
    }
    if (ret == null) {
      ErrorReport.reportDdlException(ErrorCode.ERR_WRONG_VALUE_FOR_VAR, SessionVariable.EXEC_SCHEDULING_STRATEGY, varValue);
    }
    return ret;
  }
}
