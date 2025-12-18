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

package org.apache.doris.indexpolicy;

import org.apache.doris.common.DdlException;

import com.google.common.collect.ImmutableSet;
import com.ibm.icu.text.UnicodeSet;

import java.util.Map;
import java.util.Set;

public class ICUNormalizerTokenFilterValidator extends BasePolicyValidator {
    private static final Set<String> ALLOWED_PROPS =
            ImmutableSet.of("type", "name", "unicode_set_filter");
    private static final Set<String> VALID_NAMES = ImmutableSet.of(
            "nfc", "nfd", "nfkc", "nfkd", "nfkc_cf");

    public ICUNormalizerTokenFilterValidator() {
        super(ALLOWED_PROPS);
    }

    @Override
    protected String getTypeName() {
        return "ICU normalizer filter";
    }

    @Override
    protected void validateSpecific(Map<String, String> props) throws DdlException {
        if (props.containsKey("name")) {
            String name = props.get("name").toLowerCase();
            if (!VALID_NAMES.contains(name)) {
                throw new DdlException("Invalid name '" + name + "' for ICU normalizer filter. "
                        + "Supported names: " + VALID_NAMES + " (default: nfkc_cf)");
            }
        }
        if (props.containsKey("unicode_set_filter")) {
            String filter = props.get("unicode_set_filter").trim();
            if (!filter.isEmpty()) {
                try {
                    new UnicodeSet(filter);
                } catch (IllegalArgumentException e) {
                    throw new DdlException("Invalid unicode_set_filter '" + filter
                            + "' for ICU normalizer filter. " + e.getMessage());
                }
            }
        }
    }
}
