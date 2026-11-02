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

import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * The time zone a materialized view definition pins with SET_VAR hints decides the zone its content is
 * materialized in, so every hint of the definition has to be seen - including one written with a differently
 * cased key, and one inside a scalar subquery whose plan lives on the expression - and hints that disagree
 * have to be rejected instead of silently persisting one of them.
 */
public class CreateMTMVInfoHintTest {

    private static String hintedTimeZone(String sql) {
        // building the plan of a hint applies the SET_VAR hint through the statement context of the
        // current session, so the context has to be installed before parsing and to own a statement context
        ConnectContext context = new ConnectContext();
        context.setThreadLocalInfo();
        context.setStatementContext(new StatementContext());
        try {
            LogicalPlan query = new NereidsParser().parseSingle(sql);
            return CreateMTMVInfo.hintedTimeZoneOf(query);
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    public void testNoHint() {
        Assertions.assertNull(hintedTimeZone("select date_trunc(ts, 'day') as d from t"));
    }

    @Test
    public void testLowercaseHint() {
        Assertions.assertEquals("+08:00",
                hintedTimeZone("select /*+ SET_VAR(time_zone='+08:00') */ date_trunc(ts, 'day') as d from t"));
    }

    /**
     * The hint key is matched case-insensitively, exactly like the variable application: a definition pinning
     * {@code TIME_ZONE} materializes in that zone too, so it must not look unpinned.
     */
    @Test
    public void testUppercaseHintKey() {
        Assertions.assertEquals("+08:00",
                hintedTimeZone("select /*+ SET_VAR(TIME_ZONE='+08:00') */ date_trunc(ts, 'day') as d from t"));
    }

    @Test
    public void testEquivalentZonesInSeveralScopes() {
        Assertions.assertEquals("+08:00",
                hintedTimeZone("select /*+ SET_VAR(time_zone='+08:00') */ d from "
                        + "(select /*+ SET_VAR(TIME_ZONE='GMT+8') */ date_trunc(ts, 'day') as d from t) x"));
    }

    @Test
    public void testConflictingZonesInSeveralScopes() {
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> hintedTimeZone("select /*+ SET_VAR(time_zone='+08:00') */ d from "
                        + "(select /*+ SET_VAR(time_zone='UTC') */ date_trunc(ts, 'day') as d from t) x"));
        Assertions.assertTrue(exception.getMessage().contains("conflicting time zones"), exception.getMessage());
    }

    /**
     * A scalar subquery owns its plan on the expression rather than as a child node, so its hint is only
     * reachable through the node expressions. It is still applied while the definition is parsed and analyzed,
     * so a conflicting zone there has to be rejected as well - and it must be found even though the subquery
     * folds away to a constant later.
     */
    @Test
    public void testConflictingZoneInScalarSubquery() {
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> hintedTimeZone("select d from "
                        + "(select /*+ SET_VAR(time_zone='+08:00') */ date_trunc(ts, 'day') as d from t) x "
                        + "where (select /*+ SET_VAR(time_zone='UTC') */ 1) = 1"));
        Assertions.assertTrue(exception.getMessage().contains("conflicting time zones"), exception.getMessage());

        // the same zone everywhere is still accepted
        Assertions.assertEquals("+08:00",
                hintedTimeZone("select d from (select /*+ SET_VAR(time_zone='+08:00') */"
                        + " date_trunc(ts, 'day') as d from t) x"
                        + " where (select /*+ SET_VAR(TIME_ZONE='+08:00') */ 1) = 1"));
    }
}
