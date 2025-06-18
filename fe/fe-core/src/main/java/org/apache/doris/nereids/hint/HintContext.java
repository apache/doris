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

package org.apache.doris.nereids.hint;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

/**
 * context for hint
 */
public class HintContext {
    private List<Optional<String>> qbNameHistory;

    public HintContext(Optional<String> qbName) {
        qbNameHistory = ImmutableList.of(qbName);
    }

    private HintContext(List<Optional<String>> qbNameHistory) {
        this.qbNameHistory = qbNameHistory;
    }

    public Optional<String> getQbName() {
        return !qbNameHistory.isEmpty() ? qbNameHistory.get(qbNameHistory.size() - 1) : Optional.empty();
    }

    public Optional<String> getOriginalQbName() {
        return !qbNameHistory.isEmpty() ? qbNameHistory.get(0) : Optional.empty();
    }

    /**
     * withQbName
     */
    public HintContext withQbName(Optional<String> qbName) {
        if (!qbNameHistory.isEmpty()) {
            if (!qbName.equals(qbNameHistory.get(qbNameHistory.size() - 1))) {
                ImmutableList.Builder<Optional<String>> builder = new ImmutableList.Builder<>();
                builder.addAll(qbNameHistory);
                builder.add(qbName);
                return new HintContext(builder.build());
            } else {
                return this;
            }
        } else {
            return new HintContext(qbName);
        }
    }

    /**
     * merge two QbNames
     */
    public static Optional<HintContext> merge(Optional<HintContext> oldContext, Optional<HintContext> newContext) {
        if (oldContext.isPresent()) {
            Optional<String> newQbName = newContext.isPresent() ? newContext.get().getQbName() : Optional.empty();
            return Optional.of(oldContext.get().withQbName(newQbName));
        } else {
            return newContext;
        }
    }
}
