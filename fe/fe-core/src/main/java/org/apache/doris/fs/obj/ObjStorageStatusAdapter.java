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

package org.apache.doris.fs.obj;

import org.apache.doris.backup.Status;

import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * Adapter that converts {@link Status}-based results from {@link ObjStorage}
 * to standard {@link IOException}.
 *
 * <p>This is a transitional bridge: it allows new code to work with IOExceptions
 * while the underlying ObjStorage implementation still returns Status objects.
 * Once all implementations are migrated to throw IOException directly,
 * this class can be removed.
 */
public final class ObjStorageStatusAdapter {

    private ObjStorageStatusAdapter() {}

    /**
     * Throws IOException if the status indicates failure.
     * Maps {@link Status.ErrCode#NOT_FOUND} to {@link FileNotFoundException};
     * all other errors become plain {@link IOException}.
     *
     * @param status    the status to check
     * @param operation the operation name for error message context
     * @param path      the remote path involved in the operation
     * @throws FileNotFoundException if status code is NOT_FOUND
     * @throws IOException           for all other error statuses
     */
    public static void throwIfFailed(Status status, String operation, String path)
            throws IOException {
        if (status.ok()) {
            return;
        }
        throw toIOException(status, operation, path);
    }

    /**
     * Converts a failed Status to an appropriate IOException subtype.
     * Convenience for use in lambda/stream contexts.
     */
    public static IOException toIOException(Status status, String operation, String path) {
        String msg = operation + " failed for path [" + path + "]: " + status.getErrMsg();
        if (Status.ErrCode.NOT_FOUND.equals(status.getErrCode())) {
            return new FileNotFoundException(msg);
        }
        return new IOException(msg);
    }
}
