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

package org.apache.doris.common.jni.vec;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

/**
 * How a pruned Doris {@link ColumnType} tree maps onto the data source's own type tree.
 *
 * <p>The engine can prune a complex column down to the sub-fields a query actually touches, and it
 * describes the result as a narrower {@code ColumnType}. Turning that back into something a reader can
 * act on means walking the two trees together and resolving each requested field against the source —
 * and the rules for doing so must be IDENTICAL across connectors: a fluss union read has the same
 * logical column decoded by the paimon reader for the lake half and by the fluss reader for the log
 * half, so a matching rule that differs by a hair returns half the rows wrong without failing.
 *
 * <p>What a consumer does with the result differs. Paimon rebuilds a pruned paimon type and pushes it
 * down ({@code withReadType}); fluss has no nested pushdown, so it keeps reading whole rows and uses
 * {@link #sourceChildIndex} to pick fields out of them at decode time. Both get their field resolution
 * from here.
 *
 * <p><b>Adding VARIANT.</b> This branch has no VARIANT in {@link ColumnType.Type}, so there is no
 * {@code Kind.VARIANT} here either — an unreachable branch is worse than none. Supporting it takes four
 * things, none of which exist yet: a {@code VARIANT} constant in {@code ColumnType.Type}, a
 * {@code Kind.VARIANT} here, a BE-to-Java channel carrying per-column access paths (a variant's pruned
 * shape is a list of paths and cannot be expressed as a type), and a per-connector projection built from
 * those paths.
 */
public final class NestedProjection<T> {

    /** What the requested type is, as the data source sees it. */
    public enum Kind { SCALAR, STRUCT, ARRAY, MAP }

    /** Navigates one data source's type system. Implemented once per connector; every method is a read. */
    public interface TypeSource<T> {
        Kind kindOf(T type);

        List<String> structFieldNames(T type);

        T structFieldType(T type, int index);

        T arrayElementType(T type);

        T mapKeyType(T type);

        T mapValueType(T type);

        /** Rendered into error messages only. */
        String describe(T type);
    }

    private static final int[] ELEMENT_INDEXES = new int[] {0};
    private static final int[] KEY_VALUE_INDEXES = new int[] {0, 1};

    private final Kind kind;
    private final T sourceType;
    private final ColumnType required;
    private final boolean identity;
    private final int[] sourceChildIndexes;
    private final List<NestedProjection<T>> children;

    private NestedProjection(Kind kind, T sourceType, ColumnType required, boolean identity,
            int[] sourceChildIndexes, List<NestedProjection<T>> children) {
        this.kind = kind;
        this.sourceType = sourceType;
        this.required = required;
        this.identity = identity;
        this.sourceChildIndexes = sourceChildIndexes;
        this.children = children;
    }

    /**
     * Aligns one projected column's requested type against the source type it will be read from.
     *
     * @throws IllegalArgumentException when a requested field is absent from the source, a requested
     *         field matches more than one source field ignoring case, two requested children resolve to
     *         the same source field, or the two trees disagree about a level's shape — including a Doris
     *         scalar requested over a complex source. All are hard failures rather than a fall back to
     *         reading the whole column: the requested shape was derived from this table's own schema, so
     *         a mismatch means the schema changed after planning or the caller has a bug, and quietly
     *         reading something else hands back values nobody asked for.
     */
    public static <T> NestedProjection<T> of(ColumnType required, T sourceType, TypeSource<T> source) {
        if (required.isStruct()) {
            return struct(required, sourceType, source);
        }
        if (required.isArray()) {
            return array(required, sourceType, source);
        }
        if (required.isMap()) {
            return map(required, sourceType, source);
        }
        if (required.isUnsupported()) {
            // Doris could not describe this column's type at all, so there is no shape to compare
            // against the source: hand the source type straight back rather than guess at one.
            return new NestedProjection<T>(Kind.SCALAR, sourceType, required, true, null, null);
        }
        // A scalar keeps the source type rather than the Doris one: the two type systems do not agree
        // on timestamp precision or CHAR length, and a reader picks its accessor from what the source
        // stored. The source still has to actually BE scalar-shaped — a Doris scalar requested over a
        // source STRUCT/ARRAY/MAP is the same shape conflict requireKind rejects for the complex kinds,
        // just with the two sides swapped.
        requireKind(Kind.SCALAR, required, sourceType, source);
        return new NestedProjection<T>(Kind.SCALAR, sourceType, required, true, null, null);
    }

    private static <T> NestedProjection<T> struct(ColumnType required, T sourceType, TypeSource<T> source) {
        requireKind(Kind.STRUCT, required, sourceType, source);
        List<String> requestedNames = required.getChildNames();
        List<ColumnType> requestedTypes = required.getChildTypes();
        if (requestedNames == null || requestedTypes == null
                || requestedNames.size() != requestedTypes.size()) {
            throw new IllegalArgumentException("Doris STRUCT projection for column '"
                    + required.getName() + "' has mismatched child names and types");
        }
        List<String> sourceNames = source.structFieldNames(sourceType);
        int[] indexes = new int[requestedNames.size()];
        // Java 8: a primitive array, not a Set<Integer>, so catching a collision does not box every
        // source index checked against it.
        boolean[] taken = new boolean[sourceNames.size()];
        List<NestedProjection<T>> childShapes = new ArrayList<NestedProjection<T>>(requestedNames.size());
        boolean isIdentity = requestedNames.size() == sourceNames.size();
        for (int i = 0; i < requestedNames.size(); i++) {
            int sourceIndex = indexOfIgnoreCase(sourceNames, requestedNames.get(i), required);
            if (sourceIndex < 0) {
                throw new IllegalArgumentException(String.format(
                        "Doris requested nested field '%s' of column '%s', which does not exist in %s",
                        requestedNames.get(i), required.getName(), source.describe(sourceType)));
            }
            if (taken[sourceIndex]) {
                throw new IllegalArgumentException(String.format(
                        "Doris requested field '%s' of column '%s', which resolves to the same field"
                                + " of %s as an earlier requested child. The request cannot be"
                                + " satisfied: one source field would be read twice and another not"
                                + " at all.",
                        requestedNames.get(i), required.getName(), source.describe(sourceType)));
            }
            taken[sourceIndex] = true;
            indexes[i] = sourceIndex;
            NestedProjection<T> child = of(requestedTypes.get(i),
                    source.structFieldType(sourceType, sourceIndex), source);
            childShapes.add(child);
            isIdentity = isIdentity && sourceIndex == i && child.identity;
        }
        return new NestedProjection<T>(Kind.STRUCT, sourceType, required, isIdentity, indexes,
                Collections.unmodifiableList(childShapes));
    }

    private static <T> NestedProjection<T> array(ColumnType required, T sourceType, TypeSource<T> source) {
        requireKind(Kind.ARRAY, required, sourceType, source);
        List<ColumnType> requestedTypes = required.getChildTypes();
        if (requestedTypes == null || requestedTypes.size() != 1) {
            throw new IllegalArgumentException("Doris ARRAY projection for column '"
                    + required.getName() + "' must have exactly one child type");
        }
        NestedProjection<T> element =
                of(requestedTypes.get(0), source.arrayElementType(sourceType), source);
        return new NestedProjection<T>(Kind.ARRAY, sourceType, required, element.identity,
                ELEMENT_INDEXES, Collections.singletonList(element));
    }

    private static <T> NestedProjection<T> map(ColumnType required, T sourceType, TypeSource<T> source) {
        requireKind(Kind.MAP, required, sourceType, source);
        List<ColumnType> requestedTypes = required.getChildTypes();
        if (requestedTypes == null || requestedTypes.size() != 2) {
            throw new IllegalArgumentException("Doris MAP projection for column '"
                    + required.getName() + "' must have exactly two child types");
        }
        // Key and value both go through the walker. The engine prunes only the value today, but a key is
        // resolved the same way rather than assumed identical, so a future key projection cannot land here
        // as a silent position-aligned read.
        NestedProjection<T> key = of(requestedTypes.get(0), source.mapKeyType(sourceType), source);
        NestedProjection<T> value = of(requestedTypes.get(1), source.mapValueType(sourceType), source);
        return new NestedProjection<T>(Kind.MAP, sourceType, required,
                key.identity && value.identity, KEY_VALUE_INDEXES,
                Collections.unmodifiableList(Arrays.asList(key, value)));
    }

    private static <T> void requireKind(Kind expected, ColumnType required, T sourceType,
            TypeSource<T> source) {
        Kind actual = source.kindOf(sourceType);
        if (actual != expected) {
            throw new IllegalArgumentException(String.format(
                    "Doris requested %s for column '%s', but the source type is %s (%s)",
                    expected, required.getName(), actual, source.describe(sourceType)));
        }
    }

    /**
     * An exact match always wins. Failing that, exactly one case-insensitive match is fine; two or more
     * is ambiguous and must fail loud rather than silently pick whichever one the source happened to
     * list first — that would resolve a field to the wrong column with no error at all. (Reachable in
     * practice: the encoded struct-field channel preserves original case, and the legacy one lowercases.)
     */
    private static int indexOfIgnoreCase(List<String> names, String name, ColumnType required) {
        for (int i = 0; i < names.size(); i++) {
            if (names.get(i).equals(name)) {
                return i;
            }
        }
        int match = -1;
        for (int i = 0; i < names.size(); i++) {
            if (names.get(i).toLowerCase(Locale.ROOT).equals(name.toLowerCase(Locale.ROOT))) {
                if (match >= 0) {
                    throw new IllegalArgumentException(String.format(
                            "Doris requested nested field '%s' of column '%s' matches more than one "
                                    + "source field ignoring case: '%s' and '%s'",
                            name, required.getName(), names.get(match), names.get(i)));
                }
                match = i;
            }
        }
        return match;
    }

    public Kind getKind() {
        return kind;
    }

    public T getSourceType() {
        return sourceType;
    }

    public ColumnType getRequired() {
        return required;
    }

    /**
     * Whether this subtree asks for everything the source has, in the source's own order. A consumer that
     * sees {@code true} can keep doing exactly what it did before this class existed.
     */
    public boolean isIdentity() {
        return identity;
    }

    /** How many children were requested. STRUCT only; ARRAY is always 1 and MAP always 2. */
    public int childCount() {
        return children == null ? 0 : children.size();
    }

    /** Where the requested child sits in the SOURCE type — the whole point of this class. */
    public int sourceChildIndex(int requestedIndex) {
        return sourceChildIndexes[requestedIndex];
    }

    public NestedProjection<T> child(int requestedIndex) {
        return children.get(requestedIndex);
    }

    public NestedProjection<T> elementProjection() {
        return children.get(0);
    }

    public NestedProjection<T> keyProjection() {
        return children.get(0);
    }

    public NestedProjection<T> valueProjection() {
        return children.get(1);
    }
}
