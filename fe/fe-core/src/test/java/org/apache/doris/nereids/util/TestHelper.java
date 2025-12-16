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

package org.apache.doris.nereids.util;

import org.apache.doris.nereids.trees.expressions.Expression;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Helper functions for tests.
 */
public class TestHelper {
    public static List<String> toSqlList(Collection<? extends Expression> expressions) {
        return expressions.stream().map(Expression::toSql).collect(Collectors.toList());
    }

    public static Set<String> toSqlSet(Collection<? extends Expression> expressions) {
        return expressions.stream().map(Expression::toSql).collect(Collectors.toSet());
    }

    public static List<String> toStringList(Collection<? extends Expression> expressions) {
        return expressions.stream().map(Expression::toString).collect(Collectors.toList());
    }

    public static Set<String> toStringSet(Collection<? extends Expression> expressions) {
        return expressions.stream().map(Expression::toString).collect(Collectors.toSet());
    }
}
