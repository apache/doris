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
import org.apache.doris.common.util.TimeUtils;

import org.apache.doris.mysql.MysqlHandshakePacket;

import com.google.common.collect.Lists;

import java.lang.reflect.Field;
import java.util.List;

// You can place your global variable in this class with public and VariableMgr.VarAttr annotation.
// You can get this variable from MySQL client with statement `SELECT @@variable_name`,
// and change its value through `SET variable_name = xxx`
// NOTE: If you want access your variable safe, please hold VariableMgr's lock before access.
public final class GlobalVariable {

    public static final String VERSION_COMMENT = "version_comment";
    public static final String VERSION = "version";
    public static final String LOWER_CASE_TABLE_NAMES = "lower_case_table_names";
    public static final String LICENSE = "license";
    public static final String LANGUAGE = "language";
    public static final String INIT_CONNECT = "init_connect";
    public static final String SYSTEM_TIME_ZONE = "system_time_zone";
    public static final String QUERY_CACHE_SIZE = "query_cache_size";
    public static final String DEFAULT_ROWSET_TYPE = "default_rowset_type";
    public static final String PERFORMANCE_SCHEMA = "performance_schema";

    @VariableMgr.VarAttr(name = VERSION_COMMENT, flag = VariableMgr.READ_ONLY)
    public static String versionComment = "Doris version " +
            Version.DORIS_BUILD_VERSION + "-" + Version.DORIS_BUILD_SHORT_HASH;

    @VariableMgr.VarAttr(name = VERSION, flag = VariableMgr.READ_ONLY)
    public static String version = MysqlHandshakePacket.SERVER_VERSION;

    // 0: table names are stored as specified and comparisons are case sensitive.
    // 1: table names are stored in lowercase on disk and comparisons are not case sensitive.
    // 2: table names are stored as given but compared in lowercase.
    @VariableMgr.VarAttr(name = LOWER_CASE_TABLE_NAMES, flag = VariableMgr.READ_ONLY)
    public static int lowerCaseTableNames = 0;

    @VariableMgr.VarAttr(name = LICENSE, flag = VariableMgr.READ_ONLY)
    public static String license = "Apache License, Version 2.0";

    @VariableMgr.VarAttr(name = LANGUAGE, flag = VariableMgr.READ_ONLY)
    public static String language = "/palo/share/english/";

    // A string to be executed by the server for each client that connects
    @VariableMgr.VarAttr(name = INIT_CONNECT, flag = VariableMgr.GLOBAL)
    public volatile static String initConnect = "";

    // A string to be executed by the server for each client that connects
    @VariableMgr.VarAttr(name = SYSTEM_TIME_ZONE, flag = VariableMgr.READ_ONLY)
    public static String systemTimeZone = TimeUtils.getSystemTimeZone().getID();

    // The amount of memory allocated for caching query results
    @VariableMgr.VarAttr(name = QUERY_CACHE_SIZE, flag = VariableMgr.GLOBAL)
    public volatile static long queryCacheSize = 1048576;

    @VariableMgr.VarAttr(name = DEFAULT_ROWSET_TYPE, flag = VariableMgr.GLOBAL)
    public volatile static String defaultRowsetType = "beta";

    // add performance schema to support MYSQL JDBC 8.0.16 or later versions.
    @VariableMgr.VarAttr(name = PERFORMANCE_SCHEMA, flag = VariableMgr.READ_ONLY)
    public static String performanceSchema = "OFF";

    // Don't allow create instance.
    private GlobalVariable() {
    }

    public static List<String> getPersistentGlobalVarNames() {
        List<String> varNames = Lists.newArrayList();
        for (Field field : GlobalVariable.class.getDeclaredFields()) {
            VariableMgr.VarAttr attr = field.getAnnotation(VariableMgr.VarAttr.class);
            // Since the flag of lower_case_table_names is READ_ONLY, it is handled separately here.
            if (attr != null && (attr.flag() == VariableMgr.GLOBAL || attr.name().equals(LOWER_CASE_TABLE_NAMES))) {
                varNames.add(attr.name());
            }
        }
        return varNames;
    }
}
