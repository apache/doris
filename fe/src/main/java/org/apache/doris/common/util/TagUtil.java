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

package org.apache.doris.common.util;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.resource.TagSet;

import java.util.Map;

public class TagUtil {
    private static final String PROP_TAG_PREFIX = "tag.";

    public static TagSet analyzeTagProperties(Map<String, String> properties)
            throws AnalysisException {
        TagSet userTagSet = null;
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            String key = entry.getKey().toLowerCase();
            if (key.startsWith(PROP_TAG_PREFIX)) {
                String tagType = key.substring(PROP_TAG_PREFIX.length());
                if (tagType.equals("type")) {
                    throw new AnalysisException("Can not specify 'type' tag");
                }

                TagSet tmp = TagSet.createFromString(key.substring(PROP_TAG_PREFIX.length()), entry.getValue());
                if (userTagSet == null) {
                    userTagSet = tmp;
                } else {
                    userTagSet.merge(tmp);
                }
            }
        }
        return userTagSet;
    }

}
