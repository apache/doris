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

package org.apache.doris.nereids.trees.expressions.literal;

import java.util.Optional;
import java.util.function.Supplier;

/** Result */
public class Result<R, T extends RuntimeException> {
    private final Optional<R> result;
    private final Optional<Supplier<T>> exceptionSupplier;

    private Result(Optional<R> result, Optional<Supplier<T>> exceptionSupplier) {
        this.result = result;
        this.exceptionSupplier = exceptionSupplier;
    }

    public static <R, T extends RuntimeException> Result<R, T> ok(R result) {
        return new Result<>(Optional.of(result), Optional.empty());
    }

    public static <R, T extends RuntimeException> Result<R, T> err(Supplier<T> exceptionSupplier) {
        return new Result<>(Optional.empty(), Optional.of(exceptionSupplier));
    }

    public boolean isOk() {
        return !exceptionSupplier.isPresent();
    }

    public boolean isError() {
        return exceptionSupplier.isPresent();
    }

    public <R, T extends RuntimeException> Result<R, T> cast() {
        return (Result<R, T>) this;
    }

    public R get() {
        if (exceptionSupplier.isPresent()) {
            throw exceptionSupplier.get().get();
        }
        return result.get();
    }

    public R orElse(R other) {
        if (exceptionSupplier.isPresent()) {
            return other;
        }
        return result.get();
    }
}
