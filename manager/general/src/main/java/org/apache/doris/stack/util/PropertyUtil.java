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

package org.apache.doris.stack.util;

import org.springframework.util.StringUtils;

import org.apache.doris.stack.constant.EnvironmentDefine;
import org.apache.doris.stack.constant.PropertyDefine;

import java.util.HashMap;
import java.util.Map;

/**
 * @Description：spring service startup, initializing property system configuration information
 */
public class PropertyUtil {

    private PropertyUtil() {
        throw new UnsupportedOperationException();
    }

    // When starting the program, read some environment variable values into the properties configuration
    private static final String DB_TYPE = System.getenv(EnvironmentDefine.DB_TYPE_ENV);

    private static final String DB_DBNAME = System.getenv(EnvironmentDefine.DB_DBNAME_ENV);

    private static final String DB_PORT = System.getenv(EnvironmentDefine.DB_PORT_ENV);

    private static final String DB_USER = System.getenv(EnvironmentDefine.DB_USER_ENV);

    private static final String DB_PASS = System.getenv(EnvironmentDefine.DB_PASS_ENV);

    private static final String DB_HOST = System.getenv(EnvironmentDefine.DB_HOST_ENV);

    private static final String STUDIO_PORT = System.getenv(EnvironmentDefine.STUDIO_PORT_ENV);

    private static final String NGINX_PORT = System.getenv(EnvironmentDefine.NGINX_PORT_ENV);

    private static final String STUDIO_COOKIE_MAX_AGE = System.getenv(EnvironmentDefine.STUDIO_COOKIE_MAX_AGE_ENV);

    private static final String SUPER_PASSWORD_RESER = System.getenv(EnvironmentDefine.SUPER_PASSWORD_RESER_ENV);

    private static final String MAX_LOGIN_FAILED_TIMES = System.getenv(EnvironmentDefine.MAX_LOGIN_FAILED_TIMES_ENV);

    private static final String LOGIN_DELAY_TIME = System.getenv(EnvironmentDefine.LOGIN_DELAY_TIME_ENV);

    private static final String MAX_LOGIN_TIMES_IN_FIVE_MINUTES = System.getenv(EnvironmentDefine.MAX_LOGIN_TIMES_IN_FIVE_MINUTES_ENV);

    private static final String MAX_LOGIN_TIMES = System.getenv(EnvironmentDefine.MAX_LOGIN_TIMES_ENV);

    public static Map<String, Object> getProperties() {
        Map<String, Object> properties = new HashMap<>();

        // Service port configuration
        if (STUDIO_PORT == null || STUDIO_PORT.isEmpty()) {
            properties.put(PropertyDefine.SERVER_PORT_PROPERTY, 8080);
        } else {
            properties.put(PropertyDefine.SERVER_PORT_PROPERTY, STUDIO_PORT);
        }

        // Nginx service port configuration
        if (NGINX_PORT == null || NGINX_PORT.isEmpty()) {
            properties.put(PropertyDefine.NGINX_PORT_PROPERTY, 8090);
        } else {
            properties.put(PropertyDefine.NGINX_PORT_PROPERTY, NGINX_PORT);
        }

        // Database JPA configuration
        properties.put("spring.jpa.properties.hibernate.jdbc.time_zone", "Asia/Shanghai");
        properties.put("spring.jpa.show-sql", true);
        properties.put("spring.jpa.hibernate.ddl-auto", "update");
        properties.put("spring.jpa.hibernate.naming-strategy", "org.hibernate.cfg.ImprovedNamingStrategy");
        properties.put("spring.data.jpa.repositories.enabled", true);
        properties.put("spring.jpa.hibernate.use-new-id-generator-mappings", false);
        //TODO:default mysql
        if (StringUtils.isEmpty(DB_TYPE) || DB_TYPE.equals(PropertyDefine.JPA_DATABASE_MYSQL)) {
            // Configure MySQL database access
            properties.put(PropertyDefine.JPA_DATABASE_PROPERTY, PropertyDefine.JPA_DATABASE_MYSQL);
            properties.put("spring.datasource.driverClassName", "com.mysql.cj.jdbc.Driver");
            StringBuffer url = new StringBuffer();
            url.append("jdbc:mysql://");
            if (StringUtils.isEmpty(DB_HOST)) {
                url.append("mysqlHost");
                properties.put(PropertyDefine.MYSQL_HOST_PROPERTY, "mysqlHost");
            } else {
                url.append(DB_HOST);
                properties.put(PropertyDefine.MYSQL_HOST_PROPERTY, DB_HOST);
            }
            url.append(":");
            if (StringUtils.isEmpty(DB_PORT)) {
                url.append("8306");
                properties.put(PropertyDefine.MYSQL_PORT_PROPERTY, 8306);
            } else {
                url.append(DB_PORT);
                properties.put(PropertyDefine.MYSQL_PORT_PROPERTY, DB_PORT);
            }
            url.append("/");
            if (StringUtils.isEmpty(DB_DBNAME)) {
                url.append("manager");
            } else {
                url.append(DB_DBNAME);
            }
            url.append("?useUnicode=true&characterEncoding=utf-8");

            properties.put("spring.datasource.url", url.toString());

            if (StringUtils.isEmpty(DB_USER)) {
                properties.put("spring.datasource.username", "root");
            } else {
                properties.put("spring.datasource.username", DB_USER);
            }

            if (StringUtils.isEmpty(DB_PASS)) {
                properties.put("spring.datasource.password", "password");
            } else {
                properties.put("spring.datasource.password", DB_PASS);
            }
            properties.put("spring.jpa.properties.hibernate.dialect", "org.hibernate.dialect.MySQL5InnoDBDialect");
        } else if (DB_TYPE.equals(PropertyDefine.JPA_DATABASE_H2)) {
            // Configure H2 database access
            // Relative path saved in local file
            properties.put(PropertyDefine.JPA_DATABASE_PROPERTY, PropertyDefine.JPA_DATABASE_H2);
            // properties.put("spring.datasource.url", "jdbc:h2:mem:h2test");// Save only in memory
            properties.put("spring.datasource.url", "jdbc:h2:file:./manager");
            properties.put("spring.datasource.driver-class-name", "org.h2.Driver");
            properties.put("spring.datasource.username", "userName");
            properties.put("spring.datasource.password", "passwprd");

            // Initializing table creation
            properties.put("spring.h2.console.settings.web-allow-others", true);
            properties.put("spring.h2.console.path", "/h2-console/manager");
            properties.put("spring.h2.console.enabled", true);

            properties.put("spring.jpa.properties.hibernate.dialect", "org.hibernate.dialect.H2Dialect");
        } else {
            // Configure PostgreSQL database access
            properties.put(PropertyDefine.JPA_DATABASE_PROPERTY, PropertyDefine.JPA_DATABASE_POSTGRESQL);
            properties.put("spring.datasource.driverClassName", "org.postgresql.Driver");

            StringBuffer url = new StringBuffer();
            url.append("jdbc:postgresql://");
            if (StringUtils.isEmpty(DB_HOST)) {
                url.append("postgresqlhost");
                properties.put(PropertyDefine.POSTGRESQL_HOST_PROPERTY, "postgresqlhost");
            } else {
                url.append(DB_HOST);
                properties.put(PropertyDefine.POSTGRESQL_HOST_PROPERTY, DB_HOST);
            }
            url.append(":");
            if (StringUtils.isEmpty(DB_PORT)) {
                url.append("8432");
                properties.put(PropertyDefine.POSTGRESQL_PORT_PROPERTY, 8432);
            } else {
                url.append(DB_PORT);
                properties.put(PropertyDefine.POSTGRESQL_PORT_PROPERTY, DB_PORT);
            }
            url.append("/");
            if (StringUtils.isEmpty(DB_DBNAME)) {
                url.append("postgresqlDb");
            } else {
                url.append(DB_DBNAME);
            }

            properties.put("spring.datasource.url", url.toString());

            if (StringUtils.isEmpty(DB_USER)) {
                properties.put("spring.datasource.username", "postgres");
            } else {
                properties.put("spring.datasource.username", DB_USER);
            }

            if (StringUtils.isEmpty(DB_PASS)) {
                properties.put("spring.datasource.password", "password");
            } else {
                properties.put("spring.datasource.password", DB_PASS);
            }
            properties.put("spring.jpa.properties.hibernate.dialect", "org.hibernate.dialect.PostgreSQL10Dialect");
        }

        // Log configuration
        properties.put("spring.profiles.active", "dev");
        properties.put("logging.level.org.apache.doris.stack", "debug");

        // Configure cookie validity (minutes)
        if (STUDIO_COOKIE_MAX_AGE == null) {
            // The default is 14 days
            properties.put(PropertyDefine.MAX_SESSION_AGE_PROPERTY, "20160");
        } else {
            properties.put(PropertyDefine.MAX_SESSION_AGE_PROPERTY, STUDIO_COOKIE_MAX_AGE);
        }

        // Configure whether the super administrator user password is reset
        if (SUPER_PASSWORD_RESER == null) {
            // The default is false
            properties.put(PropertyDefine.SUPER_USER_PASS_RESET_PROPERTY, false);
        } else {
            properties.put(PropertyDefine.SUPER_USER_PASS_RESET_PROPERTY, SUPER_PASSWORD_RESER);
        }

        // Configure maximum failed logins
        if (MAX_LOGIN_FAILED_TIMES == null) {
            // The default is 5 times
            properties.put(PropertyDefine.MAX_LOGIN_FAILED_TIMES_PROPERTY, 5);
        } else {
            properties.put(PropertyDefine.MAX_LOGIN_FAILED_TIMES_PROPERTY, MAX_LOGIN_FAILED_TIMES);
        }

        // Failed to configure login account disabling time
        if (LOGIN_DELAY_TIME == null) {
            // The default is 5 minutes
            properties.put(PropertyDefine.LOGIN_DELAY_TIME_PROPERTY, 300000);
        } else {
            properties.put(PropertyDefine.LOGIN_DELAY_TIME_PROPERTY, LOGIN_DELAY_TIME);
        }

        //Configure the number of people online within five minutes
        if (MAX_LOGIN_TIMES_IN_FIVE_MINUTES == null) {
            // The default is 1000
            properties.put(PropertyDefine.MAX_LOGIN_TIMES_IN_FIVE_MINUTES_PROPERTY, 1000);
        } else {
            properties.put(PropertyDefine.MAX_LOGIN_TIMES_IN_FIVE_MINUTES_PROPERTY, MAX_LOGIN_TIMES_IN_FIVE_MINUTES);
        }

        // Configure the number of people online at the same time
        if (MAX_LOGIN_TIMES == null) {
            // The default is 5000
            properties.put(PropertyDefine.MAX_LOGIN_TIMES_PROPERTY, 5000);
        } else {
            properties.put(PropertyDefine.MAX_LOGIN_TIMES_PROPERTY, MAX_LOGIN_TIMES);
        }

        return properties;
    }

}
