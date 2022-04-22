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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/AliasGenerator.java
// and modified by Doris

package org.apache.doris.common;

import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

/**
 * Abstract class representing an alias generator. It uses a prefix and a
 * monotonically increasing counter to generate new aliases. Classes extending
 * this class are responsible for initializing the prefix.
 */
public abstract class AliasGenerator {
    private int numGeneratedAliases = 1;
    protected String aliasPrefix = null;
    protected Set<String> usedAliases = Sets.newHashSet();

    /**
     * Return the next available alias.
     */
    public String getNextAlias() {
        Preconditions.checkNotNull(aliasPrefix);
        while (true) {
            String candidateAlias = aliasPrefix + Integer.toString(numGeneratedAliases++);
            if (usedAliases.add(candidateAlias)) {
                // add success
                return candidateAlias;
            }
            if (numGeneratedAliases < 0) {
                throw new IllegalStateException("Overflow occurred during alias generation.");
            }
        }
    }
}

