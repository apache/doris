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

package org.apache.doris.nereids.pattern.generator.javaast;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

/** java's qualified name. */
public class QualifiedName implements JavaAstNode {
    public final List<String> identifiers;

    public QualifiedName(List<String> identifiers) {
        this.identifiers = ImmutableList.copyOf(identifiers);
    }

    public boolean suffixIs(String name) {
        return !identifiers.isEmpty() && identifiers.get(identifiers.size() - 1).equals(name);
    }

    /** get suffix name. */
    public Optional<String> suffix() {
        if (identifiers.isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of(identifiers.get(identifiers.size() - 1));
        }
    }

    @Override
    public String toString() {
        return Joiner.on(".").join(identifiers);
    }
}
