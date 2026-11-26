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

package org.apache.doris.connector.spi;

import org.apache.doris.connector.spi.handle.ConnectorTableHandle;

/**
 * Reading a format that a plugin declares in {@link ConnectorProvider#getFileFormats()} through the file table
 * functions, such as {@code s3("uri" = ..., "format" = "<name>")}.
 *
 * <p>Implemented by the metadata of the connector that {@link ConnectorProvider#createFileFormatConnector}
 * builds; a catalog's connector never needs it. The engine asks for a handle while it analyzes the statement,
 * once per table function. It then reads the columns with {@link ConnectorMetadata#getTableSchema} and plans the
 * scan with the connector's scan-plan provider, both on that handle, as it does for a catalog table.</p>
 */
public interface ConnectorFileSourceOps {

    /**
     * Resolves what a file table function names and pins what the statement reads: a format with versions pins
     * the version it finds here, so that the columns and every range of the scan describe the same data.
     *
     * @throws DorisConnectorException with a message for the user when the location holds nothing of this
     *         format, or cannot be read with the storage the source carries
     */
    ConnectorTableHandle getFileTableHandle(ConnectorSession session, ConnectorFileSource source);
}
