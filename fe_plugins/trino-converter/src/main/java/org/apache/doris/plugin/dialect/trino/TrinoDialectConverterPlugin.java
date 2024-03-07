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

package org.apache.doris.plugin.dialect.trino;

import org.apache.doris.analysis.StatementBase;
import org.apache.doris.nereids.parser.Dialect;
import org.apache.doris.plugin.DialectConverterPlugin;
import org.apache.doris.plugin.Plugin;
import org.apache.doris.qe.SessionVariable;

import com.google.common.collect.ImmutableSet;

import javax.annotation.Nullable;
import java.util.List;

/**
 * Extends from {@link org.apache.doris.plugin.DialectConverterPlugin}, focus on Trino dialect.
 */
public class TrinoDialectConverterPlugin extends Plugin implements DialectConverterPlugin {

    @Override
    public ImmutableSet<Dialect> acceptDialects() {
        return ImmutableSet.of(Dialect.TRINO);
    }

    @Override
    public @Nullable List<StatementBase> parseSqlWithDialect(String sql, SessionVariable sessionVariable) {
        return TrinoParser.parse(sql, sessionVariable);
    }
}
