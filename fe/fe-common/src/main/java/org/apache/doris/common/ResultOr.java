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

public class ResultOr<T, E> {
    private final T value;

    private final E error;

    private final boolean isSuccess;

    private ResultOr(T value, E error, boolean isSuccess) {
        this.value = value;
        this.error = error;
        this.isSuccess = isSuccess;
    }

    public static <T, E> ResultOr<T, E> ok(T value) {
        return new ResultOr<>(value, null, true);
    }

    public static <T, E> ResultOr<T, E> err(E error) {
        return new ResultOr<>(null, error, false);
    }

    public boolean isOk() {
        return isSuccess;
    }

    public boolean isErr() {
        return !isSuccess;
    }

    public T unwrap() {
        if (!isSuccess) {
            throw new IllegalStateException("Tried to unwrap Err: " + error);
        }
        return value;
    }

    public E unwrapErr() {
        if (isSuccess) {
            throw new IllegalStateException("Tried to unwrapOk");
        }
        return error;
    }

    @Override
    public String toString() {
        return isSuccess ? "Ok(" + value + ")" : "Err(" + error + ")";
    }
}
