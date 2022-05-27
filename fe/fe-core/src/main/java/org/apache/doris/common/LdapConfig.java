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

/**
 * LDAP configuration
 */
public class LdapConfig extends ConfigBase {

    /**
     * Flag to enable LDAP authentication.
     */
    @ConfigBase.ConfField
    public static boolean ldap_authentication_enabled = false;

    /**
     * LDAP server ip.
     */
    @ConfigBase.ConfField
    public static String ldap_host = "";

    /**
     * LDAP server port.
     */
    @ConfigBase.ConfField
    public static int ldap_port = 389;

    /**
     * Search base for users.
     * LDAP is a tree structure, and this specifies the base of the subtree in which the search is to be constrained.
     */
    @ConfigBase.ConfField
    public static String ldap_user_basedn = "";

    /**
     * The DN to bind as connection, this value will be used to lookup information about other users.
     */
    @ConfigBase.ConfField
    public static String ldap_admin_name = "";

    /**
     * User lookup filter, the placeholder {login} will be replaced by the user supplied login.
     */
    @ConfigBase.ConfField
    public static String ldap_user_filter = "(&(uid={login}))";

    /**
     * Search base for groups.
     */
    @ConfigBase.ConfField
    public static String ldap_group_basedn = "";

    /**
     * Maximum number of user connections. This value should be between 1 and 10000.
     */
    @ConfigBase.ConfField
    public static long user_max_connections = 100L;

    /**
     * LDAP pool configuration:
     * https://docs.spring.io/spring-ldap/docs/2.3.3.RELEASE/reference/#pool-configuration
     */
    /**
     * The maximum number of active connections of each type (read-only or read-write) that can be allocated
     * from this pool at the same time. You can use a non-positive number for no limit.
     */
    @ConfigBase.ConfField
    public static int max_active = 8;

    /**
     * The overall maximum number of active connections (for all types) that can be allocated from this pool
     * at the same time. You can use a non-positive number for no limit.
     */
    @ConfigBase.ConfField
    public static int max_total = -1;

    /**
     * The maximum number of active connections of each type (read-only or read-write) that can remain idle
     * in the pool without extra connections being released. You can use a non-positive number for no limit.
     */
    @ConfigBase.ConfField
    public static int max_idle = 8;

    /**
     * The minimum number of active connections of each type (read-only or read-write) that can remain idle
     * in the pool without extra connections being created. You can use zero (the default) to create none.
     */
    @ConfigBase.ConfField
    public static int min_idle = 0;

    /**
     * The maximum number of milliseconds that the pool waits (when no connections are available) for a connection
     * to be returned before throwing an exception. You can use a non-positive number to wait indefinitely.
     */
    @ConfigBase.ConfField
    public static int max_wait = -1;

    /**
     * Specifies the behavior when the pool is exhausted.
     *
     * The '0' option throws NoSuchElementException when the pool is exhausted.
     *
     * The '1' option waits until a new object is available. If max-wait is positive and no new object is available
     * after the max-wait time expires, NoSuchElementException is thrown.
     *
     * The '2' option creates and returns a new object (essentially making max-active meaningless).
     */
    @ConfigBase.ConfField
    public static byte when_exhausted = 1;

    /**
     * Whether objects are validated before being borrowed from the pool. If the object fails to validate,
     * it is dropped from the pool, and an attempt to borrow another is made.
     */
    @ConfigBase.ConfField
    public static boolean test_on_borrow = false;

    /**
     * Whether objects are validated before being returned to the pool.
     */
    @ConfigBase.ConfField
    public static boolean test_on_return = false;

    /**
     * Whether objects are validated by the idle object evictor (if any). If an object fails to validate,
     * it is dropped from the pool.
     */
    @ConfigBase.ConfField
    public static boolean test_while_idle = false;
}
