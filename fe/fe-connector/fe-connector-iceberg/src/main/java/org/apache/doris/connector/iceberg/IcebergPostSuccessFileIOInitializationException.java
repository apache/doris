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

import org.apache.commons.lang3.exception.ExceptionUtils;

import java.util.Optional;

/**
 * A create/register response was received successfully, but its local FileIO configuration could not be bound.
 * This is not a failed or unknown Iceberg commit, and the completed REST request must not be retried.
 */
final class IcebergPostSuccessFileIOInitializationException extends RuntimeException {

    enum Operation {
        CREATE("Iceberg REST create-table request succeeded, but local FileIO initialization failed; "
                + "do not retry the create request automatically."),
        STAGE_CREATE("Iceberg REST stage-create request succeeded, but local FileIO initialization failed; "
                + "the table transaction has not been committed. Do not retry the preparation request automatically."),
        REGISTER("Iceberg REST register-table request succeeded, but local FileIO initialization failed; "
                + "do not retry the register request automatically.");

        private final String diagnostic;

        Operation(String diagnostic) {
            this.diagnostic = diagnostic;
        }
    }

    IcebergPostSuccessFileIOInitializationException(Operation operation, RuntimeException cause) {
        // Keep the cause for debugging, but never copy a credential-bearing input or cause message into the diagnostic.
        super(operation.diagnostic, cause);
    }

    static Optional<IcebergPostSuccessFileIOInitializationException> find(Throwable failure) {
        return ExceptionUtils.getThrowableList(failure).stream()
                .filter(IcebergPostSuccessFileIOInitializationException.class::isInstance)
                .map(IcebergPostSuccessFileIOInitializationException.class::cast)
                .findFirst();
    }
}
