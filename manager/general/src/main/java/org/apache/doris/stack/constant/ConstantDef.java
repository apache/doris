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

package org.apache.doris.stack.constant;

import com.google.common.collect.Maps;

import java.util.Map;

/**
 * @Description：Uniform constant definition
 */
public class ConstantDef {

    // General constant definition
    // Separator
    public static final String UNDERLINE = "_";

    // The related name definition of the default metadata of the Doris engine
    //Default namespace for Doris engine
    public static final String DORIS_DEFAULT_NS = "default_cluster";

    // MySQL (Doris is the same as MySQL) has its own default metabase name
    public static final String MYSQL_DEFAULT_SCHEMA = "information_schema";

    // MySQL (Doris is consistent with MySQL) has its own default metabase ID,
    // which does not really exist in the metabase
    public static final int MYSQL_SCHEMA_DB_ID = -1;

    // Metadata information table of MySQL (Doris is consistent with MySQL)
    public static final Map<Integer, String> MYSQL_METADATA_TABLE;
    static {
        MYSQL_METADATA_TABLE = Maps.newHashMap();
        MYSQL_METADATA_TABLE.put(-1, "character_sets");
        MYSQL_METADATA_TABLE.put(-2, "collations");
        MYSQL_METADATA_TABLE.put(-3, "columns");
        MYSQL_METADATA_TABLE.put(-4, "engines");
        MYSQL_METADATA_TABLE.put(-5, "global_variables");
        MYSQL_METADATA_TABLE.put(-6, "key_column_usage");
        MYSQL_METADATA_TABLE.put(-7, "referential_constraints");
        MYSQL_METADATA_TABLE.put(-8, "routines");
        MYSQL_METADATA_TABLE.put(-9, "schema_privileges");
        MYSQL_METADATA_TABLE.put(-10, "schemata");
        MYSQL_METADATA_TABLE.put(-11, "session_variables");
        MYSQL_METADATA_TABLE.put(-12, "statistics");
        MYSQL_METADATA_TABLE.put(-13, "table_constraints");
        MYSQL_METADATA_TABLE.put(-14, "table_privileges");
        MYSQL_METADATA_TABLE.put(-15, "tables");
        MYSQL_METADATA_TABLE.put(-16, "user_privileges");
        MYSQL_METADATA_TABLE.put(-17, "views");
    }

    // user constant definition
    // By default, there is no space ID assigned
    public static final int NON_CLUSTER = -1;

    // user_id of super administrator in session table
    public static final int SUPER_USER_ID = 0;

    // The default user group name when the user does not have any space
    public static final String USER_DEFAULT_GROUP_NAME = "Default";

    // Front end request tag name
    public static final String REQUEST_INDENTIFIER = "Request-Id";

    // Log builder Key definition
    // Log unique request identifier key
    public static final String LOG_INDENTIFIER = "RequestId";

    // log message key
    public static final String LOG_MESSAGE_KEY = "msg";

    // log exception key
    public static final String LOG_EXCEPTION_KEY = "exception";

    // user Id
    public static final String LOG_USER_ID_KEY = "user_id";

    // user name
    public static final String LOG_USER_NAME_KEY = "user_name";

    // user login session id
    public static final String LOG_SESSION_KEY = "session_id";

    // doris space（Cluster）id
    public static final String LOG_CLUSTER_ID_KEY = "cluster_id";

    // database id
    public static final String LOG_DATABASE_ID_KEY = "database_id";

    // table id
    public static final String LOG_TABLE_ID_KEY = "table_id";

    // field id
    public static final String LOG_FIELD_ID_KEY = "field_id";

    // new token mark
    public static final String NEW_TOKEN_MARK = "new#";

}
