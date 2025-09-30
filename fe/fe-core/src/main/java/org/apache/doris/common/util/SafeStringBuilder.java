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

package org.apache.doris.common.util;

import org.apache.doris.common.profile.Profile;

import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SafeStringBuilder {
    private StringBuilder builder = new StringBuilder();
    @Getter
    private long maxCapacity;
    @Getter
    private boolean truncated = false;
    private Logger log = LogManager.getLogger(Profile.class);

    public SafeStringBuilder() {
        this(Integer.MAX_VALUE);
    }

    public SafeStringBuilder(int maxCapacity) {
        if (maxCapacity < 16) {
            log.warn("SafeStringBuilder max capacity {} must be greater than 16", maxCapacity);
            maxCapacity = 16;
        }
        this.maxCapacity = maxCapacity - 16;
    }

    public SafeStringBuilder append(String str) {
        if (!truncated) {
            if (builder.length() + str.length() <= maxCapacity) {
                builder.append(str);
            } else {
                log.warn("Append str truncated, builder length(): {}, str length: {}, max capacity: {}",
                        builder.length(), str.length(), maxCapacity);
                builder.append(str, 0, (int) (maxCapacity - builder.length()));
                markTruncated();
            }
        }
        return this;
    }

    public SafeStringBuilder append(Object obj) {
        return append(String.valueOf(obj));
    }

    public int length() {
        return builder.length();
    }

    public String toString() {
        if (truncated) {
            return builder.toString() + "\n...[TRUNCATED]";
        }
        return builder.toString();
    }

    private void markTruncated() {
        truncated = true;
        log.warn("SafeStringBuilder exceeded max capacity {}", maxCapacity);
    }
}
