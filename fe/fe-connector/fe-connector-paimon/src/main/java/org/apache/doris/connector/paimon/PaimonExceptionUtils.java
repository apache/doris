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

package org.apache.doris.connector.paimon;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.paimon.fs.UnsupportedSchemeException;

import java.util.Arrays;
import java.util.stream.Collectors;

final class PaimonExceptionUtils {

    private PaimonExceptionUtils() {
    }

    static RuntimeException catalogCreationFailure(String prefix, Exception failure) {
        for (Throwable cause : ExceptionUtils.getThrowableList(failure)) {
            if (cause instanceof UnsupportedSchemeException && cause.getSuppressed().length > 0) {
                // FileIO.get reports failed access probes as an unsupported scheme, even when the
                // implementation exists. The actual IO failures are attached as suppressed exceptions.
                Throwable[] accessFailures = cause.getSuppressed();
                String details = Arrays.stream(accessFailures)
                        .map(ExceptionUtils::getRootCauseMessage)
                        .collect(Collectors.joining("; "));
                RuntimeException wrapped = new RuntimeException(prefix + ": Failed to initialize Paimon FileIO. "
                        + "Access failures: " + details + ". FileIO resolution: " + cause.getMessage(),
                        accessFailures[0]);
                // Root-cause consumers (including SHOW CATALOGS) must reach an actual access failure.
                // Retain the original aggregate and all of its failed attempts in the diagnostic stack.
                wrapped.addSuppressed(failure);
                return wrapped;
            }
        }
        return new RuntimeException(prefix + ": " + failure.getMessage(), failure);
    }
}
