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

package org.apache.doris.connector.iceberg;

import org.apache.doris.connector.spi.DorisConnectorException;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.iceberg.exceptions.NotFoundException;

import java.io.FileNotFoundException;

final class IcebergExceptionUtils {

    private IcebergExceptionUtils() {
    }

    static RuntimeException wrapTableLoadFailure(
            IcebergTableHandle handle, Exception failure, String fallbackPrefix) {
        // Iceberg and FileIO implementations wrap missing metadata differently. Inspect every cause so all
        // read paths preserve the stable table-format contract instead of exposing a filesystem wrapper message.
        boolean metadataNotFound = ExceptionUtils.getThrowableList(failure).stream()
                .anyMatch(cause -> cause instanceof NotFoundException
                        || cause instanceof FileNotFoundException);
        if (metadataNotFound) {
            return new DorisConnectorException("Metadata not found in metadata location for table "
                    + handle.getDbName() + "." + handle.getTableName(), failure);
        }
        return new RuntimeException(fallbackPrefix + failure.getMessage(), failure);
    }
}
