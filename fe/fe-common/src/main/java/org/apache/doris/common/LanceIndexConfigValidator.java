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

package org.apache.doris.common;

import java.lang.reflect.Field;

/**
 * Validators for the Lance index admission configuration items in {@link Config}. The
 * unresolved-job quotas and the static bounds must stay positive (section 9.7 enabling
 * precondition), so every handler rejects a zero, negative, or unparsable value before
 * assigning. The handlers extend {@link ConfigBase.DefaultConfHandler} and delegate to
 * {@code super.handle} after validation: a bare {@code ConfHandler} implementation would
 * validate without ever assigning, letting ADMIN SET FRONTEND CONFIG pass with no effect.
 * Note the callback only runs on the ADMIN SET path; values loaded from fe.conf bypass it
 * ({@code ConfigBase.setFields} assigns directly), so the admission path re-asserts the
 * positive invariant where the values are consumed.
 */
public final class LanceIndexConfigValidator {
    private LanceIndexConfigValidator() {
    }

    /**
     * Accepts only a positive long: validates first, then assigns via the default handler.
     */
    public static class PositiveLongConfigHandler extends ConfigBase.DefaultConfHandler {
        @Override
        public void handle(Field field, String confVal) throws Exception {
            String trimmedVal = confVal == null ? "" : confVal.trim();
            final long value;
            try {
                value = Long.parseLong(trimmedVal);
            } catch (NumberFormatException e) {
                throw new ConfigException(field.getName() + " must be a positive long, but got: " + confVal, e);
            }
            if (value <= 0) {
                throw new ConfigException(field.getName() + " must be a positive long, but got: " + confVal);
            }
            super.handle(field, trimmedVal);
        }
    }

    /**
     * Accepts only a positive int: validates first, then assigns via the default handler.
     */
    public static class PositiveIntConfigHandler extends ConfigBase.DefaultConfHandler {
        @Override
        public void handle(Field field, String confVal) throws Exception {
            String trimmedVal = confVal == null ? "" : confVal.trim();
            final int value;
            try {
                value = Integer.parseInt(trimmedVal);
            } catch (NumberFormatException e) {
                throw new ConfigException(field.getName() + " must be a positive int, but got: " + confVal, e);
            }
            if (value <= 0) {
                throw new ConfigException(field.getName() + " must be a positive int, but got: " + confVal);
            }
            super.handle(field, trimmedVal);
        }
    }
}
