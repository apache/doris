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

package org.apache.doris.nereids.trees.expressions.literal.format;

import java.util.Objects;
import java.util.function.Predicate;

/** DebugChecker */
public class DebugChecker<T extends FormatChecker> extends FormatChecker {
    private final T childChecker;
    private final Predicate<T> debugPoint;

    public DebugChecker(String name, T childChecker, Predicate<T> debugPoint) {
        super(name);
        this.childChecker = Objects.requireNonNull(childChecker, "childChecker can not be null");
        this.debugPoint = Objects.requireNonNull(debugPoint, "debugPoint can not be null");
    }

    @Override
    protected boolean doCheck(StringInspect stringInspect) {
        return debugPoint.test(childChecker);
    }
}
