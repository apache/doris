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

package org.apache.doris.datasource.property.storage;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.util.JsonUtil;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

/**
 * Parses the {@code uris} property of an external-file table-valued function into a list of
 * location URIs.
 *
 * <p>{@code uris} is a JSON array naming several datasets at once. The parser validates only the
 * JSON shape of that array and throws {@link AnalysisException} when the input is malformed: a
 * non-array, an empty array, more than {@link #MAX_URIS} elements, or a non-string / blank element.
 * Duplicate elements are collapsed while the written order is preserved.
 */
public final class Uris {

    /** The property key for {@code uris}. */
    public static final String KEY = "uris";

    /**
     * The maximum number of elements allowed in {@code uris}; a longer array is refused at parse time.
     */
    public static final int MAX_URIS = 10_000;

    private Uris() {
    }

    /**
     * Parses {@code raw} into the URIs it names, in the order written and without duplicates.
     *
     * @throws AnalysisException if {@code raw} is not a JSON array, is empty, has more than
     *         {@link #MAX_URIS} elements, or contains a non-string or blank element - the message
     *         identifies the offending part
     */
    public static List<String> parse(String raw) throws AnalysisException {
        if (raw == null || raw.trim().isEmpty()) {
            throw new AnalysisException("'" + KEY + "' must be a JSON array of URIs, "
                    + "for example '[\"s3://bucket/a\", \"s3://bucket/b\"]'");
        }
        JsonNode root;
        try {
            root = JsonUtil.readTree(raw);
        } catch (RuntimeException e) {
            throw new AnalysisException("'" + KEY + "' is not valid JSON: it must be a JSON array of "
                    + "URIs, for example '[\"s3://bucket/a\", \"s3://bucket/b\"]'");
        }
        if (root == null || !root.isArray()) {
            throw new AnalysisException("'" + KEY + "' must be a JSON array of URIs");
        }
        if (root.isEmpty()) {
            throw new AnalysisException("'" + KEY + "' must name at least one location; "
                    + "the array is empty");
        }
        if (root.size() > MAX_URIS) {
            throw new AnalysisException("'" + KEY + "' must not exceed " + MAX_URIS
                    + " elements, but got " + root.size());
        }

        LinkedHashSet<String> uris = new LinkedHashSet<>();
        for (int index = 0; index < root.size(); index++) {
            JsonNode element = root.get(index);
            if (element == null || !element.isTextual()) {
                throw new AnalysisException("'" + KEY + "' element at index " + index
                        + " must be a URI string");
            }
            String uri = element.asText();
            if (uri.trim().isEmpty()) {
                throw new AnalysisException("'" + KEY + "' element at index " + index
                        + " must not be empty");
            }
            uris.add(uri);
        }
        return new ArrayList<>(uris);
    }
}
