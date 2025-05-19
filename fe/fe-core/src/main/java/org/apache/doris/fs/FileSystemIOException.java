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

package org.apache.doris.fs;

import org.apache.doris.backup.Status.ErrCode;

import java.io.IOException;
import java.util.Optional;
import javax.annotation.Nullable;

public class FileSystemIOException extends IOException {

    @Nullable
    private ErrCode errCode;

    public FileSystemIOException(ErrCode errCode, String message) {
        super(message);
        this.errCode = errCode;
    }

    public FileSystemIOException(ErrCode errCode, String message, Throwable cause) {
        super(message, cause);
        this.errCode = errCode;
    }

    public FileSystemIOException(String message) {
        super(message);
        this.errCode = null;
    }

    public FileSystemIOException(String message, Throwable cause) {
        super(message, cause);
        this.errCode = null;
    }

    public Optional<ErrCode> getErrorCode() {
        return Optional.ofNullable(errCode);
    }

    @Override
    public String getMessage() {
        if (errCode != null) {
            return String.format("[%s]: %s",
                    errCode,
                    super.getMessage());
        } else {
            return super.getMessage();
        }
    }
}
