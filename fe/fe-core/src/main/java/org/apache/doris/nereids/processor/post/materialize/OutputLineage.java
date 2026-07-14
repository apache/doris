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

package org.apache.doris.nereids.processor.post.materialize;

import org.apache.doris.nereids.trees.expressions.Slot;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Immutable lineage description for one plan output. */
public final class OutputLineage {
    /** Supported lineage shapes. */
    public enum Kind {
        DIRECT,
        FORWARDED,
        DERIVED
    }

    private final Kind kind;
    private final List<Slot> inputs;
    private final MaterializeSource source;

    private OutputLineage(Kind kind, List<Slot> inputs, MaterializeSource source) {
        this.kind = Objects.requireNonNull(kind, "kind must not be null");
        this.inputs = ImmutableList.copyOf(inputs);
        this.source = source;
    }

    public static OutputLineage direct(MaterializeSource source) {
        MaterializeSource checkedSource = Objects.requireNonNull(source, "source must not be null");
        return new OutputLineage(Kind.DIRECT, ImmutableList.of(checkedSource.getBaseSlot()), checkedSource);
    }

    public static OutputLineage forwarded(Slot input, MaterializeSource source) {
        return new OutputLineage(Kind.FORWARDED, ImmutableList.of(input),
                Objects.requireNonNull(source, "source must not be null"));
    }

    public static OutputLineage derived(List<Slot> inputs) {
        return new OutputLineage(Kind.DERIVED, inputs, null);
    }

    public Kind getKind() {
        return kind;
    }

    public List<Slot> getInputs() {
        return inputs;
    }

    public Optional<MaterializeSource> getSource() {
        return Optional.ofNullable(source);
    }
}
