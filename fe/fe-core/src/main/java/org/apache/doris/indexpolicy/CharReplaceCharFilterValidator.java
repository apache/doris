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

import org.apache.doris.analysis.InvertedIndexUtil;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;

import com.google.common.collect.ImmutableSet;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class CharReplaceCharFilterValidator extends BasePolicyValidator {
    private static final Set<String> ALLOWED_PROPS = ImmutableSet.of(
            "type", "pattern", "replacement");

    public CharReplaceCharFilterValidator() {
        super(ALLOWED_PROPS);
    }

    @Override
    protected String getTypeName() {
        return "char_replace filter";
    }

    @Override
    protected void validateSpecific(Map<String, String> props) throws DdlException {
        Map<String, String> charFilterProperties = new HashMap<>();
        charFilterProperties.put(InvertedIndexUtil.INVERTED_INDEX_PARSER_CHAR_FILTER_TYPE,
                InvertedIndexUtil.INVERTED_INDEX_CHAR_FILTER_CHAR_REPLACE);
        if (props.containsKey("pattern")) {
            charFilterProperties.put(InvertedIndexUtil.INVERTED_INDEX_PARSER_CHAR_FILTER_PATTERN, props.get("pattern"));
        }
        if (props.containsKey("replacement")) {
            charFilterProperties.put(InvertedIndexUtil.INVERTED_INDEX_PARSER_CHAR_FILTER_REPLACEMENT,
                    props.get("replacement"));
        }
        try {
            InvertedIndexUtil.checkCharFilterProperties(charFilterProperties);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage(), e);
        }
    }
}
