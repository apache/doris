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

package org.apache.doris.extension.loader;

import java.nio.file.Path;
import java.util.Objects;

/**
 * Standard failure record for plugin loading.
 */
public final class LoadFailure {

    public static final String STAGE_SCAN = "scan";
    public static final String STAGE_RESOLVE = "resolve";
    public static final String STAGE_CREATE_CLASSLOADER = "createClassLoader";
    public static final String STAGE_DISCOVER = "discover";
    public static final String STAGE_INSTANTIATE = "instantiate";
    public static final String STAGE_CONFLICT = "conflict";

    private final Path pluginDir;
    private final String stage;
    private final String message;
    private final Throwable cause;

    public LoadFailure(Path pluginDir, String stage, String message, Throwable cause) {
        this.pluginDir = pluginDir;
        this.stage = requireNonBlank(stage, "stage");
        this.message = requireNonBlank(message, "message");
        this.cause = cause;
    }

    public Path getPluginDir() {
        return pluginDir;
    }

    public String getStage() {
        return stage;
    }

    public String getMessage() {
        return message;
    }

    public Throwable getCause() {
        return cause;
    }

    private static String requireNonBlank(String value, String fieldName) {
        Objects.requireNonNull(value, fieldName);
        String trimmed = value.trim();
        if (trimmed.isEmpty()) {
            throw new IllegalArgumentException(fieldName + " is blank");
        }
        return trimmed;
    }
}
