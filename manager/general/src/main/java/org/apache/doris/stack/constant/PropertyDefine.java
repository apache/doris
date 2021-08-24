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
 * @Description：properties System configuration name and content definition
 */
public class PropertyDefine {

    private PropertyDefine() {
        throw new UnsupportedOperationException();
    }

    // Configuration item name definition
    // Application name configuration item
    public static final String DEPLOY_TYPE_PROPERTY = "deploy.type";

    // Application background storage database type configuration item
    public static final String JPA_DATABASE_PROPERTY = "spring.jpa.database";

    // Application port configuration item
    public static final String SERVER_PORT_PROPERTY = "server.port";

    // Application nginx service port configuration item
    public static final String NGINX_PORT_PROPERTY = "nginx.port";

    // Connection address configuration item of MySQL engine
    public static final String MYSQL_HOST_PROPERTY = "mysql.host";

    // Connection port configuration item of MySQL engine
    public static final String MYSQL_PORT_PROPERTY = "mysql.port";

    // Connection address configuration item of PostgreSQL engine
    public static final String POSTGRESQL_HOST_PROPERTY = "postgresql.host";

    // Connection port configuration item of PostgreSQL engine
    public static final String POSTGRESQL_PORT_PROPERTY = "postgresql.port";

    public static final String MAX_SESSION_AGE_PROPERTY = "max.session.age";

    public static final String SUPER_USER_PASS_RESET_PROPERTY = "super.user.password.reset";

    public static final String MAX_LOGIN_FAILED_TIMES_PROPERTY = "max.login.failed.times";

    public static final String LOGIN_DELAY_TIME_PROPERTY = "login.delay.time";

    public static final String MAX_LOGIN_TIMES_IN_FIVE_MINUTES_PROPERTY = "max.login.times.in.five.minutes";

    public static final String MAX_LOGIN_TIMES_PROPERTY = "max.login.times";

    // Definition of special configuration item value
    // Application name configuration item value, studio application name
    public static final String DEPLOY_TYPE_STUDIO = "studio";

    // Application name, configuration item value, manager application name
    public static final String DEPLOY_TYPE_MANAGER = "manager";

    // The application background uses the storage database type configuration item value, MySQL database
    public static final String JPA_DATABASE_MYSQL = "mysql";

    // The application background uses the storage database type configuration item value, H2 database
    public static final String JPA_DATABASE_H2 = "h2";

    // The application background uses the storage database type configuration item value, and PostgreSQL database
    public static final String JPA_DATABASE_POSTGRESQL = "postgresql";

}
