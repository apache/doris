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

package org.apache.doris.nereids.trees.plans.commands.info;

/**
 * Represents the information needed to drop a tag in the system.
 *
 */
public class DropTagInfo {

    private final String tagName;
    private final Boolean ifExists;

    public DropTagInfo(String tagName, boolean ifExists) {
        this.tagName = tagName;
        this.ifExists = ifExists;
    }

    public String getTagName() {
        return tagName;
    }

    public Boolean getIfExists() {
        return ifExists;
    }

    /**
     * Generates the SQL representation of the drop tag command.
     *
     * @return SQL string for drop a tag
     */
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("DROP TAG");
        if (ifExists) {
            sb.append(" IF EXISTS");
        }
        sb.append(" ").append(tagName);
        return sb.toString();
    }
}
