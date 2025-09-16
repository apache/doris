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

package org.apache.doris.nereids.util;

import java.util.function.Supplier;

/** LazyCompute */
public class LazyCompute<T> implements Supplier<T> {
    private volatile Result<T> result;
    private final Supplier<T> supplier;

    private LazyCompute(Supplier<T> supplier) {
        this.supplier = supplier;
        this.result = Result.NOT_COMPUTED;
    }

    private LazyCompute(T value) {
        this.supplier = null;
        this.result = new Result<>(value);
    }

    public static <T> LazyCompute<T> of(Supplier<T> supplier) {
        return new LazyCompute<>(supplier);
    }

    public static <T> LazyCompute<T> ofInstance(T value) {
        return new LazyCompute<>(value);
    }

    @Override
    public T get() {
        if (result != Result.NOT_COMPUTED) {
            return result.value;
        }
        synchronized (this) {
            if (result == Result.NOT_COMPUTED) {
                result = new Result<>(supplier.get());
            }
            return result.value;
        }
    }

    private static class Result<T> {
        public static final Result NOT_COMPUTED = new Result<>(null);
        private final T value;

        public Result(T value) {
            this.value = value;
        }
    }
}
