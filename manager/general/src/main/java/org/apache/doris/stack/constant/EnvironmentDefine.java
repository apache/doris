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

/**
 * @Description：Defines the name of the environment variable that can be read when the service starts
 * Some of these environment variables are written to the system configuration properties to
 * initialize the service container.
 * In addition, when initializing the configuration, it will be persisted and stored
 * in the configuration table setting of the service.
 * A small part will be written into the properties and also stored in the configuration table setting,
 * such as the storage engine type used by the service.
 */
public class EnvironmentDefine {

    private EnvironmentDefine() {
        throw new UnsupportedOperationException();
    }

    // Environment variable name definition
    // Data storage engine type environment variable
    public static final String DB_TYPE_ENV = "MB_DB_TYPE";

    // Datastore database name environment variable
    public static final String DB_DBNAME_ENV = "MB_DB_DBNAME";

    // Data storage connection port environment variable
    public static final String DB_PORT_ENV = "MB_DB_PORT";

    // Data storage connection port environment variable
    public static final String DB_USER_ENV = "MB_DB_USER";

    // Data storage connection port environment variable
    public static final String DB_PASS_ENV = "MB_DB_PASS";

    // Data storage connection port environment variable
    public static final String DB_HOST_ENV = "MB_DB_HOST";

    // Data storage connection port environment variable
    public static final String STUDIO_PORT_ENV = "STUDIO_PORT";

    // Data storage connection port environment variable
    public static final String NGINX_PORT_ENV = "NGINX_PORT";

    // Set cookie validity environment variable
    public static final String STUDIO_COOKIE_MAX_AGE_ENV = "STUDIO_COOKIE_MAX_AGE";

    // Data storage connection port environment variable
    public static final String SUPER_PASSWORD_RESER_ENV = "SUPER_PASSWORD_RESER";

    // Data storage connection port environment variable
    public static final String MAX_LOGIN_FAILED_TIMES_ENV = "MAX_LOGIN_FAILED_TIMES";

    // Data storage connection port environment variable
    public static final String LOGIN_DELAY_TIME_ENV = "LOGIN_DELAY_TIME";

    // Data storage connection port environment variable
    public static final String MAX_LOGIN_TIMES_IN_FIVE_MINUTES_ENV = "MAX_LOGIN_TIMES_IN_FIVE_MINUTES";

    // Data storage connection port environment variable
    public static final String MAX_LOGIN_TIMES_ENV = "MAX_LOGIN_TIMES";

    public static final String ADMIN_EMAIL_ENV = "DS_ADMIN_EMAIL";

    public static final String ANON_TRACKING_ENABLED_ENV = "DS_ANON_TRACKING_ENABLED";

    public static final String REDIRECT_HTTPS_ENV = "DS_REDIRECT_ALL_REQUESTS_TO_HTTPS";

    public static final String SITE_LOCALE_ENV = "DS_SITE_LOCALE";

    public static final String SITE_NAME_ENV = "DS_SITE_NAME";

    public static final String SITE_URL_ENV = "DS_SITE_URL";

    public static final String VERSION_INFO_ENV = "DS_VERSION_INFO";

    public static final String DEFAULT_GROUP_ID_ENV = "DS_DEFAULT_GROUP_ID";

    public static final String ENABLE_PUBLIC_KEY_ENV = "ENABLE_PUBLIC_SHARING";

    public static final String CUSTOM_FORMATTING_KEY_ENV = "CUSTOM_FORMATTING";

    public static final String SAMPLE_DATA_ENABLE_KEY_ENV = "SAMPLE_DATA_ENABLE";

}
