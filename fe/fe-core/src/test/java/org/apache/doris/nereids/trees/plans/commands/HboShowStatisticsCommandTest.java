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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.analysis.RedirectStatus;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Unit test of the FE a {@code HBO SHOW STATISTICS} statement runs on. The pinned entries live in the
 * in-memory cache of one FE - {@code HBO SET STATISTICS} writes the cache of the FE the session is
 * connected to and a query reads it there - so the statement must not be forwarded to the master, whose
 * cache was loaded once at startup and does not contain what this session just set.
 */
public class HboShowStatisticsCommandTest {

    @Test
    public void testShowStatisticsRunsOnTheFeOfTheSession() {
        HboShowStatisticsCommand command = new HboShowStatisticsCommand("all", false, null);
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, command.toRedirectStatus());
        // the regression this test pins down: forwarding would report the cache of the master
        Assertions.assertNotEquals(RedirectStatus.FORWARD_NO_SYNC, command.toRedirectStatus());
    }
}
