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

package org.apache.doris.resource.workloadschedpolicy;

import org.apache.doris.common.UserException;

import org.apache.commons.lang3.StringUtils;

public class WorkloadConditionCompareUtils {

    static WorkloadConditionOperator getOperator(String op) throws UserException {
        if ("=".equals(op)) {
            return WorkloadConditionOperator.EQUAL;
        } else if (">".equals(op)) {
            return WorkloadConditionOperator.GREATER;
        } else if (">=".equals(op)) {
            return WorkloadConditionOperator.GREATER_EQUAL;
        } else if ("<".equals(op)) {
            return WorkloadConditionOperator.LESS;
        } else if ("<=".equals(op)) {
            return WorkloadConditionOperator.LESS_EQUAl;
        } else {
            throw new UserException("unexpected compare operator " + op);
        }
    }

    // used for select tvf
    static String getOperatorStr(WorkloadConditionOperator op) {
        switch (op) {
            case EQUAL:
                return "=";
            case GREATER:
                return ">";
            case GREATER_EQUAL:
                return ">=";
            case LESS:
                return "<";
            case LESS_EQUAl:
                return "<=";
            default:
                throw new RuntimeException("unexpected compare operator " + op);
        }
    }

    static boolean compareInteger(WorkloadConditionOperator operator, long firstArgs, long secondArgs) {
        switch (operator) {
            case EQUAL:
                return firstArgs == secondArgs;
            case GREATER:
                return firstArgs > secondArgs;
            case GREATER_EQUAL:
                return firstArgs >= secondArgs;
            case LESS:
                return firstArgs < secondArgs;
            case LESS_EQUAl:
                return firstArgs <= secondArgs;
            default:
                throw new RuntimeException("unexpected integer operator " + operator);
        }
    }

    static boolean compareDouble(WorkloadConditionOperator operator, double firstArgs, double secondArgs) {
        switch (operator) {
            case EQUAL:
                return firstArgs == secondArgs;
            case GREATER:
                return firstArgs > secondArgs;
            case GREATER_EQUAL:
                return firstArgs >= secondArgs;
            case LESS:
                return firstArgs < secondArgs;
            case LESS_EQUAl:
                return firstArgs <= secondArgs;
            default:
                throw new RuntimeException("unexpected compare double operator " + operator);
        }
    }

    static boolean compareString(WorkloadConditionOperator operator, String firstArgs, String secondArgs) {
        switch (operator) {
            case EQUAL:
                return StringUtils.equals(firstArgs, secondArgs);
            default:
                throw new RuntimeException("unexpected compare string operator " + operator);
        }
    }

}
