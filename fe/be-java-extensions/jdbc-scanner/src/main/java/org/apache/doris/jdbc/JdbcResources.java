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

package org.apache.doris.jdbc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Releases the JDBC resources a scanner, writer or connection tester holds.
 *
 * <p>The connection these classes borrow from the HikariCP pool goes back to the pool only when it
 * is closed. A connection that is never closed keeps its pool entry IN_USE forever, and once every
 * entry of a pool is held like that, every later query on the catalog waits for a connection until
 * it times out. So the connection must be closed even when closing the result set or the statement
 * fails first, which is common once a driver has aborted the connection, and even when the open
 * that borrowed it fails: BE does not call close() on a scanner or writer whose open() failed.
 */
final class JdbcResources {
    private static final Logger LOG = LoggerFactory.getLogger(JdbcResources.class);

    private JdbcResources() {
    }

    /**
     * Closes {@code resource}, logging rather than throwing if that fails, so that the caller can
     * go on to close the resources after it. Closing a closed JDBC resource is a no-op.
     */
    static void closeQuietly(AutoCloseable resource, String description) {
        if (resource == null) {
            return;
        }
        try {
            resource.close();
        } catch (Exception e) {
            LOG.warn("Failed to close " + description + ": " + e.getMessage(), e);
        }
    }
}
