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

package org.apache.doris.nereids.errors;

import org.apache.doris.common.profile.SummaryProfile;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.qe.SessionVariable;

/**
 * Exception packaging to improve code readability.
 */
public class QueryPlanningErrors {

    /**
     * plan timeout error message.
     *
     * @param elapsedS plan use time, unit is second
     * @param timeoutS timeout, unit is second
     * @param profile summary profile used to get time consumption details
     *
     * @return exception with timeout message
     */
    public static AnalysisException planTimeoutError(long elapsedS, long timeoutS, SummaryProfile profile) {
        long parseTime = profile.getParseSqlTimeMs();
        String planTime = profile.getPlanTime();
        return new AnalysisException(String.format("Nereids cost too much time (%ss > %ss)."
                        + " You should increment timeout by set '%s'"
                        + " or disable check timeout by set '%s' to false."
                        + " Time consuming details,"
                        + " parse time: %dms, plan time: %s",
                elapsedS, timeoutS,
                SessionVariable.NEREIDS_TIMEOUT_SECOND, SessionVariable.ENABLE_NEREIDS_TIMEOUT,
                parseTime, planTime));
    }
}
