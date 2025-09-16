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
 * Represents the information needed to create or replace a tag in the system.
 *
 */
public class CreateOrReplaceTagInfo {

    private final String tagName;
    private final TagOptions tagOptions;
    private final Boolean create;
    private final Boolean replace;
    private final Boolean ifNotExists;

    public CreateOrReplaceTagInfo(String tagName,
                                  boolean create,
                                  boolean replace,
                                  boolean ifNotExists,
                                  TagOptions tagOptions) {
        this.tagName = tagName;
        this.create = create;
        this.replace = replace;
        this.ifNotExists = ifNotExists;
        this.tagOptions = tagOptions;
    }

    public String getTagName() {
        return tagName;
    }

    public TagOptions getTagOptions() {
        return tagOptions;
    }

    public Boolean getCreate() {
        return create;
    }

    public Boolean getReplace() {
        return replace;
    }

    public Boolean getIfNotExists() {
        return ifNotExists;
    }

    /**
     * Generates the SQL representation of the create or replace tag command.
     *
     * @return SQL string for creating or replacing a tag
     */
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        if (create && replace) {
            sb.append("CREATE OR REPLACE TAG");
        } else if (create) {
            sb.append("CREATE TAG");
        } else if (replace) {
            sb.append("REPLACE TAG");
        }
        if (ifNotExists) {
            sb.append(" IF NOT EXISTS");
        }
        sb.append(" ").append(tagName);
        if (tagOptions != null) {
            sb.append(tagOptions.toSql());
        }
        return sb.toString();
    }
}
