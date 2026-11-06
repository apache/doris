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

package org.apache.doris.connector.adbc;

import org.apache.doris.connector.spi.DorisConnectorException;

import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcInfoCode;
import org.apache.arrow.adbc.core.AdbcStatusCode;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;

import java.util.Locale;

/** Verifies the native Flight driver's server-info isolation before it can mutate vendor state. */
final class AdbcDriverInfo {
    // Private local GetInfo capability, advertised by the bundled ADBC source patch; never sent to a server.
    private static final int CONNECTION_SCOPED_VENDOR_INFO = 0x444f0001;

    private AdbcDriverInfo() {
    }

    static void requireIsolatedVendorInfo(AdbcConnection connection) throws Exception {
        Object name;
        try {
            name = readInfo(connection, AdbcInfoCode.DRIVER_NAME.getValue());
        } catch (AdbcException e) {
            if (e.getStatus() != AdbcStatusCode.NOT_IMPLEMENTED && e.getStatus() != AdbcStatusCode.NOT_FOUND) {
                throw e;
            }
            return;
        }
        if (name == null || !name.toString().toLowerCase(Locale.ROOT).contains("flight sql")) {
            return;
        }
        // An old Flight library writes its process-wide info map during GetInfo(VENDOR_NAME).
        // A Java probe lock cannot protect ordinary native readers or prevent cross-server fallback.
        Object isolated = readInfo(connection, CONNECTION_SCOPED_VENDOR_INFO);
        if (!Boolean.TRUE.equals(isolated)) {
            throw new DorisConnectorException("The native Flight SQL driver must support connection-scoped vendor"
                    + " information. Rebuild the driver with the Doris ADBC connection-info patch and deploy the"
                    + " same library on FE and BE hosts before discovering metadata.");
        }
    }

    private static Object readInfo(AdbcConnection connection, int requestedCode) throws Exception {
        try (ArrowReader reader = connection.getInfo(new int[] {requestedCode})) {
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            while (reader.loadNextBatch()) {
                for (int row = 0; row < root.getRowCount(); row++) {
                    Object code = root.getVector("info_name").getObject(row);
                    if (code instanceof Number && ((Number) code).intValue() == requestedCode) {
                        return root.getVector("info_value").getObject(row);
                    }
                }
            }
        }
        return null;
    }
}
