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

package org.apache.doris.extension.loader;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;

/**
 * Classloading policy for plugin runtime.
 *
 * <p>Mandatory parent-first prefixes always contain
 * {@link ChildFirstClassLoader#DEFAULT_PARENT_FIRST_PACKAGES}.
 * Business modules may append additional parent-first prefixes.
 */
public final class ClassLoadingPolicy {

    private final List<String> mandatoryParentFirstPrefixes;
    private final List<String> businessParentFirstPrefixes;

    public ClassLoadingPolicy(List<String> businessParentFirstPrefixes) {
        this(null, businessParentFirstPrefixes);
    }

    public ClassLoadingPolicy(List<String> mandatoryParentFirstPrefixes, List<String> businessParentFirstPrefixes) {
        LinkedHashSet<String> mandatory = new LinkedHashSet<>(ChildFirstClassLoader.DEFAULT_PARENT_FIRST_PACKAGES);
        addPrefixes(mandatory, mandatoryParentFirstPrefixes);

        LinkedHashSet<String> business = new LinkedHashSet<>();
        addPrefixes(business, businessParentFirstPrefixes);

        this.mandatoryParentFirstPrefixes = Collections.unmodifiableList(new ArrayList<>(mandatory));
        this.businessParentFirstPrefixes = Collections.unmodifiableList(new ArrayList<>(business));
    }

    public static ClassLoadingPolicy defaultPolicy() {
        return new ClassLoadingPolicy(null, null);
    }

    public List<String> getMandatoryParentFirstPrefixes() {
        return mandatoryParentFirstPrefixes;
    }

    public List<String> getBusinessParentFirstPrefixes() {
        return businessParentFirstPrefixes;
    }

    public List<String> toParentFirstPackages() {
        LinkedHashSet<String> merged = new LinkedHashSet<>(mandatoryParentFirstPrefixes);
        merged.addAll(businessParentFirstPrefixes);
        return new ArrayList<>(merged);
    }

    private static void addPrefixes(LinkedHashSet<String> target, List<String> prefixes) {
        if (prefixes == null) {
            return;
        }
        for (String prefix : prefixes) {
            if (prefix == null) {
                continue;
            }
            String trimmed = prefix.trim();
            if (!trimmed.isEmpty()) {
                target.add(trimmed);
            }
        }
    }
}
