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

package org.apache.doris.cdcclient.source.reader.postgres;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PostgresWalLagCalculatorTest {

    @Test
    void lagQueryUsesReplicationSlotAndReturnsBytes() throws Exception {
        AtomicReference<String> actualSlotName = new AtomicReference<>();

        long lag =
                PostgresWalLagCalculator.calculate(
                        "doris_cdc_123",
                        slotName -> {
                            actualSlotName.set(slotName);
                            return new BigDecimal("4294967298");
                        });

        assertThat(actualSlotName).hasValue("doris_cdc_123");
        assertThat(lag).isEqualTo(4294967298L);
    }

    @Test
    void negativeWalDifferenceReportsUnavailable() throws Exception {
        long lag =
                PostgresWalLagCalculator.calculate(
                        "doris_cdc_123", slotName -> new BigDecimal("-100"));

        assertThat(lag).isEqualTo(-1);
    }

    @Test
    void nonIntegralWalDifferenceIsReportedToTheCaller() {
        assertThatThrownBy(
                        () ->
                                PostgresWalLagCalculator.calculate(
                                        "doris_cdc_123",
                                        slotName -> new BigDecimal("1.5")))
                .isInstanceOf(ArithmeticException.class);
    }

    @Test
    void queryFailureIsReportedToTheCaller() {
        assertThatThrownBy(
                        () ->
                                PostgresWalLagCalculator.calculate(
                                        "doris_cdc_123",
                                        slotName -> {
                                            throw new SQLException("query failed");
                                        }))
                .isInstanceOf(SQLException.class)
                .hasMessage("query failed");
    }
}
