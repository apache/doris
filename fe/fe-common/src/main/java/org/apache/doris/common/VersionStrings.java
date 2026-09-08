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

package org.apache.doris.common;

import org.apache.doris.qe.GlobalVariable;

/**
 * Centralized version strings for SQL- and variable-facing Doris metadata.
 */
public final class VersionStrings {

    private VersionStrings() {
    }

    public static String mysqlProtocolVersion() {
        return GlobalVariable.DEFAULT_SERVER_VERSION;
    }

    public static String currentVersion() {
        return Version.DORIS_BUILD_VERSION_PREFIX + " " + Version.DORIS_BUILD_VERSION;
    }

    /**
     * Stable contract:
     * "<prefix> <version>; commit=<short-hash>; build_time=<timestamp>; mysql_protocol=<version>; cloud_mode=<bool>"
     *
     * Fields are separated by "; " in a fixed order. New fields may only be appended.
     */
    public static String versionLong() {
        return currentVersion()
                + "; commit=" + Version.DORIS_BUILD_SHORT_HASH
                + "; build_time=" + Version.DORIS_BUILD_TIME
                + "; mysql_protocol=" + mysqlProtocolVersion()
                + "; cloud_mode=" + Config.isCloudMode();
    }
}
