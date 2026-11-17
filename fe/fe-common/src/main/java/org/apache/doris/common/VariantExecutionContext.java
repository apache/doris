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

import java.util.function.BooleanSupplier;

/** Statement/task-local execution policy; it never changes the persisted Variant path encoding. */
public final class VariantExecutionContext {
    private static final ThreadLocal<BooleanSupplier> CONTEXT_POLICY = new ThreadLocal<>();
    private static final ThreadLocal<Boolean> FORCE_V2 = new ThreadLocal<>();

    private VariantExecutionContext() {
    }

    public static boolean useV2() {
        if (Config.enable_variant_v2 || Boolean.TRUE.equals(FORCE_V2.get())) {
            return true;
        }
        BooleanSupplier policy = CONTEXT_POLICY.get();
        return policy != null && policy.getAsBoolean();
    }

    public static void setContextPolicy(BooleanSupplier policy) {
        if (policy == null) {
            CONTEXT_POLICY.remove();
        } else {
            CONTEXT_POLICY.set(policy);
        }
    }

    public static Scope forceV2() {
        return new Scope();
    }

    public static final class Scope implements AutoCloseable {
        private final Boolean previous;

        private Scope() {
            previous = FORCE_V2.get();
            FORCE_V2.set(true);
        }

        @Override
        public void close() {
            if (previous == null) {
                FORCE_V2.remove();
            } else {
                FORCE_V2.set(previous);
            }
        }
    }
}
