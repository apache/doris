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

package org.apache.doris.filesystem.spi;

import java.io.IOException;

/**
 * Abstraction for Hadoop-style privileged execution (Kerberos doAs).
 *
 * Defined here in fe-filesystem-spi with zero external dependencies, using
 * {@link IOCallable} instead of {@code java.security.PrivilegedExceptionAction}
 * to avoid Hadoop API dependency. Implementations live in fe-core (Kerberos/Simple
 * authenticators) and are injected into DFSFileSystem at construction time.
 *
 * P3.0a: This interface replaces the Hadoop-dependent HadoopAuthenticator from
 * fe-common for use in fe-filesystem-hdfs module.
 */
public interface HadoopAuthenticator {

    /**
     * Executes the given action under this authenticator's security context
     * (e.g., as a specific Kerberos principal or simple user).
     *
     * @param action the IO operation to execute
     * @param <T>    the return type
     * @return the result of the action
     * @throws IOException if the action throws or authentication fails
     */
    <T> T doAs(IOCallable<T> action) throws IOException;
}
