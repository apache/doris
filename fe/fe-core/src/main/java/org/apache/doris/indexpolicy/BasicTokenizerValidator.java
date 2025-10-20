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

import java.util.Map;
import java.util.Set;

public class BasicTokenizerValidator extends BasePolicyValidator {
    private static final Set<String> ALLOWED_PROPS = ImmutableSet.of("type", "extra_chars");

    public BasicTokenizerValidator() {
        super(ALLOWED_PROPS);
    }

    @Override
    protected String getTypeName() {
        return "basic tokenizer";
    }

    @Override
    protected void validateSpecific(Map<String, String> props) throws DdlException {
        if (props.containsKey("extra_chars")) {
            String extraChars = props.get("extra_chars");
            if (extraChars != null && !extraChars.isEmpty()) {
                for (int i = 0; i < extraChars.length(); i++) {
                    char c = extraChars.charAt(i);
                    if (c > 127) {
                        throw new DdlException("Invalid extra_chars for basic tokenizer: "
                                + "contains non-ASCII character '"
                                + c + "' at position " + i
                                + ". Only ASCII characters (0-127) are allowed.");
                    }
                }
            }
        }
    }
}
