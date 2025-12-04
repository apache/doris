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

package org.apache.doris.cdcclient.constants;

public class LoadConstants {
    public static final String JDBC_URL = "jdbc_url";
    public static final String DRIVER_URL = "driver_url";
    public static final String DRIVER_CLASS = "driver_class";
    public static final String USER = "user";
    public static final String PASSWORD = "password";
    public static final String DATABASE = "database";
    public static final String INCLUDE_TABLES = "include_tables";
    public static final String EXCLUDE_TABLES = "exclude_tables";
    // initial,earliest-offset,latest-offset,specific-offset,timestamp,snapshot
    public static final String STARTUP_MODE = "startup_mode";
    public static final String SPLIT_SIZE = "split_size";
    public static final String TABLE_PROPS_PREFIX = "table.create.properties.";

    public static final String DELETE_SIGN_KEY = "__DORIS_DELETE_SIGN__";
}
