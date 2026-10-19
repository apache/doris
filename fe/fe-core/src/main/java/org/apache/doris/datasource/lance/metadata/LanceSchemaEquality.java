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

package org.apache.doris.datasource.lance.metadata;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;

/**
 * Decides whether two Lance dataset schemas may be read as one table.
 *
 * <p>The answer is strict equality: the first difference found is reported, and nothing is widened,
 * reordered or merged. The comparison runs on the Arrow schema rather than on the converted Doris
 * columnsd.
 */
public final class LanceSchemaEquality {

    /** The Arrow metadata key holding a Lance extension name. */
    private static final String ARROW_EXTENSION_NAME = "ARROW:extension:name";

    private LanceSchemaEquality() {
    }

    /**
     * The first way {@code actual} differs from {@code reference}, or empty when they are equal.
     *
     * <p>The message names the offending field by path, so a nested difference says where it is.
     */
    public static Optional<String> firstDifference(Schema reference, Schema actual) {
        if (reference == null || actual == null) {
            return Optional.of("one dataset has no schema");
        }
        List<Field> expectedFields = reference.getFields();
        List<Field> actualFields = actual.getFields();
        if (expectedFields.size() != actualFields.size()) {
            return Optional.of("field count " + expectedFields.size() + " vs " + actualFields.size());
        }
        for (int index = 0; index < expectedFields.size(); index++) {
            Optional<String> difference = difference(expectedFields.get(index),
                    actualFields.get(index), "field " + index);
            if (difference.isPresent()) {
                return difference;
            }
        }
        return Optional.empty();
    }

    private static Optional<String> difference(Field expected, Field actual, String path) {
        if (!expected.getName().equals(actual.getName())) {
            return Optional.of(path + ": name '" + expected.getName() + "' vs '" + actual.getName() + "'");
        }
        if (expected.isNullable() != actual.isNullable()) {
            return Optional.of(path + " '" + expected.getName() + "': nullable "
                    + expected.isNullable() + " vs " + actual.isNullable());
        }
        Optional<String> typeDifference = typeDifference(expected.getType(), actual.getType(), path,
                expected.getName());
        if (typeDifference.isPresent()) {
            return typeDifference;
        }
        boolean expectedDictionary = expected.getDictionary() != null;
        boolean actualDictionary = actual.getDictionary() != null;
        if (expectedDictionary != actualDictionary) {
            return Optional.of(path + " '" + expected.getName() + "': dictionary encoding "
                    + (expectedDictionary ? "present" : "absent") + " vs "
                    + (actualDictionary ? "present" : "absent"));
        }
        String expectedExtension = extensionName(expected);
        String actualExtension = extensionName(actual);
        if (!Objects.equals(expectedExtension, actualExtension)) {
            return Optional.of(path + " '" + expected.getName() + "': extension name "
                    + expectedExtension + " vs " + actualExtension);
        }
        List<Field> expectedChildren = expected.getChildren();
        List<Field> actualChildren = actual.getChildren();
        if (expectedChildren.size() != actualChildren.size()) {
            return Optional.of(path + " '" + expected.getName() + "': nested field count "
                    + expectedChildren.size() + " vs " + actualChildren.size());
        }
        for (int index = 0; index < expectedChildren.size(); index++) {
            Optional<String> childDifference = difference(expectedChildren.get(index),
                    actualChildren.get(index), path + " '" + expected.getName() + "'." + index);
            if (childDifference.isPresent()) {
                return childDifference;
            }
        }
        return Optional.empty();
    }

    /** Compares the type; {@code ArrowType.equals} covers its parameters too. */
    private static Optional<String> typeDifference(ArrowType expected, ArrowType actual, String path,
            String name) {
        if (!Objects.equals(expected, actual)) {
            return Optional.of(path + " '" + name + "': type " + expected + " vs " + actual);
        }
        return Optional.empty();
    }

    private static String extensionName(Field field) {
        if (field.getMetadata() == null) {
            return null;
        }
        String extension = field.getMetadata().get(ARROW_EXTENSION_NAME);
        return extension == null ? null : extension.toLowerCase(Locale.ROOT);
    }
}
