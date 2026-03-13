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

package org.apache.doris.mysql.authenticate;

import com.google.common.base.Strings;

/**
 * Fallback policy for config-driven authentication chain execution.
 */
public enum AuthenticationChainFallbackPolicy {
    DISABLED,
    USER_NOT_FOUND,
    ANY_FAILURE;

    public static AuthenticationChainFallbackPolicy fromConfig(String configValue) {
        if (Strings.isNullOrEmpty(configValue)) {
            return DISABLED;
        }
        switch (configValue.trim().toLowerCase()) {
            case "user_not_found":
                return USER_NOT_FOUND;
            case "any_failure":
                return ANY_FAILURE;
            case "disabled":
            default:
                return DISABLED;
        }
    }
}
