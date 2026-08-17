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
        if (isMetadataNotFound(failure)) {
            return metadataNotFound(handle, failure);
        }
        return new RuntimeException(fallbackPrefix + failure.getMessage(), failure);
    }

    static RuntimeException wrapMetadataReadFailure(
            IcebergTableHandle handle, RuntimeException failure) {
        if (isMetadataNotFound(failure)) {
            return metadataNotFound(handle, failure);
        }
        return failure;
    }

    private static boolean isMetadataNotFound(Throwable failure) {
        // Iceberg and FileIO implementations wrap missing metadata differently. Inspect every cause so eager
        // planners and background lazy split iteration preserve one stable table-scoped error contract.
        return ExceptionUtils.getThrowableList(failure).stream()
                .anyMatch(cause -> cause instanceof NotFoundException
                        || cause instanceof FileNotFoundException);
    }

    private static DorisConnectorException metadataNotFound(
            IcebergTableHandle handle, Throwable failure) {
        return new DorisConnectorException("Metadata not found in metadata location for table "
                + handle.getDbName() + "." + handle.getTableName(), failure);
    }
}
