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

package org.apache.doris.nereids.pattern;

import java.util.function.Predicate;

/**
 * A predicate wrapper with a human-readable description.
 * Used in pattern matching to provide better diagnostic messages when a predicate fails.
 */
public class DescribedPredicate<T> implements Predicate<T> {
    private final String description;
    private final Predicate<T> delegate;

    public DescribedPredicate(String description, Predicate<T> delegate) {
        this.description = description;
        this.delegate = delegate;
    }

    public static <T> DescribedPredicate<T> of(String description, Predicate<T> delegate) {
        return new DescribedPredicate<>(description, delegate);
    }

    @Override
    public boolean test(T t) {
        return delegate.test(t);
    }

    public String getDescription() {
        return description;
    }

    @Override
    public String toString() {
        return description;
    }
}
