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

import org.apache.doris.common.Version;

import java.time.ZoneId;

// You can place your global variable in this class with public and VariableMgr.VarAttr annotation.
// You can get this variable from MySQL client with statement `SELECT @@variable_name`,
// and change its value through `SET variable_name = xxx`
// NOTE: If you want access your variable safe, please hold VariableMgr's lock before access.
public final class GlobalVariable {
    @VariableMgr.VarAttr(name = "version_comment", flag = VariableMgr.READ_ONLY)
    public static String versionComment = "Doris version " + Version.PALO_BUILD_VERSION;

    @VariableMgr.VarAttr(name = "version", flag = VariableMgr.READ_ONLY)
    public static String version = "5.1.0";

    // 0: table names are stored as specified and comparisons are case sensitive.
    // 1: table names are stored in lowercase on disk and comparisons are not case sensitive.
    // 2: table names are stored as given but compared in lowercase.
    @VariableMgr.VarAttr(name = "lower_case_table_names", flag = VariableMgr.READ_ONLY)
    public static int lowerCaseTableNames = 0;

    @VariableMgr.VarAttr(name = "license", flag = VariableMgr.READ_ONLY)
    public static String license = "Apache License, Version 2.0";

    @VariableMgr.VarAttr(name = "language", flag = VariableMgr.READ_ONLY)
    public static String language = "/palo/share/english/";

    // A string to be executed by the server for each client that connects
    @VariableMgr.VarAttr(name = "init_connect")
    private static String initConnect = "";

    // A string to be executed by the server for each client that connects
    @VariableMgr.VarAttr(name = "system_time_zone", flag = VariableMgr.READ_ONLY)
    public static String systemTimeZone = ZoneId.systemDefault().normalized().toString();

    // The amount of memory allocated for caching query results
    @VariableMgr.VarAttr(name = "query_cache_size")
    private static long queryCacheSize = 1048576;

    // Don't allow create instance.
    private GlobalVariable() {

    }
}
