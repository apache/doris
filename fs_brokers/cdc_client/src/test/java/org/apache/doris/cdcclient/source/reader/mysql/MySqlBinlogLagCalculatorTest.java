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

package org.apache.doris.cdcclient.source.reader.mysql;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MySqlBinlogLagCalculatorTest {

    @Test
    void sameFileLagUsesPositionsInsteadOfActiveFileSize() {
        Map<String, String> reference = offset("mysql-bin.000003", 1000);
        Map<String, String> end = offset("mysql-bin.000003", 1250);
        List<MySqlBinlogLagCalculator.BinlogFile> files =
                Collections.singletonList(
                        new MySqlBinlogLagCalculator.BinlogFile("mysql-bin.000003", 5000));

        assertThat(MySqlBinlogLagCalculator.calculate(reference, end, files)).isEqualTo(250);
    }

    @Test
    void crossFileLagIncludesTailIntermediateFilesAndHeadPosition() {
        Map<String, String> reference = offset("custom-prefix.000001", 1500);
        Map<String, String> end = offset("custom-prefix.000003", 700);
        List<MySqlBinlogLagCalculator.BinlogFile> files =
                Arrays.asList(
                        new MySqlBinlogLagCalculator.BinlogFile("custom-prefix.000001", 2000),
                        new MySqlBinlogLagCalculator.BinlogFile("custom-prefix.000002", 3000),
                        new MySqlBinlogLagCalculator.BinlogFile("custom-prefix.000003", 5000));

        assertThat(MySqlBinlogLagCalculator.calculate(reference, end, files)).isEqualTo(4200);
    }

    @Test
    void equalOffsetsReportCaughtUp() {
        Map<String, String> offset = offset("mysql-bin.000003", 1250);

        assertThat(
                        MySqlBinlogLagCalculator.calculate(
                                offset,
                                offset,
                                Collections.singletonList(
                                        new MySqlBinlogLagCalculator.BinlogFile(
                                                "mysql-bin.000003", 5000))))
                .isZero();
    }

    @Test
    void purgedReferenceFileReportsUnavailable() {
        assertThat(
                        MySqlBinlogLagCalculator.calculate(
                                offset("mysql-bin.000001", 1500),
                                offset("mysql-bin.000003", 700),
                                Arrays.asList(
                                        new MySqlBinlogLagCalculator.BinlogFile(
                                                "mysql-bin.000002", 3000),
                                        new MySqlBinlogLagCalculator.BinlogFile(
                                                "mysql-bin.000003", 5000))))
                .isEqualTo(-1);
    }

    @Test
    void missingEndFileReportsUnavailable() {
        assertThat(
                        MySqlBinlogLagCalculator.calculate(
                                offset("mysql-bin.000001", 1500),
                                offset("mysql-bin.000003", 700),
                                Arrays.asList(
                                        new MySqlBinlogLagCalculator.BinlogFile(
                                                "mysql-bin.000001", 2000),
                                        new MySqlBinlogLagCalculator.BinlogFile(
                                                "mysql-bin.000002", 3000))))
                .isEqualTo(-1);
    }

    @Test
    void referenceAheadOfHeadReportsUnavailable() {
        assertThat(
                        MySqlBinlogLagCalculator.calculate(
                                offset("mysql-bin.000003", 1500),
                                offset("mysql-bin.000003", 1250),
                                Collections.singletonList(
                                        new MySqlBinlogLagCalculator.BinlogFile(
                                                "mysql-bin.000003", 5000))))
                .isEqualTo(-1);
    }

    @Test
    void gtidOnlyReferenceReportsUnavailableUntilFilePositionIsCommitted() {
        Map<String, String> reference =
                Collections.singletonMap("gtids", "24bc7850-2c16-11ef-a0c9-0242ac120002:1-9");

        assertThat(
                        MySqlBinlogLagCalculator.calculate(
                                reference,
                                offset("mysql-bin.000003", 1250),
                                Collections.singletonList(
                                        new MySqlBinlogLagCalculator.BinlogFile(
                                                "mysql-bin.000003", 5000))))
                .isEqualTo(-1);
    }

    @Test
    void overflowIsReportedToTheCaller() {
        assertThatThrownBy(
                        () ->
                                MySqlBinlogLagCalculator.calculate(
                                        offset("mysql-bin.000001", 0),
                                        offset("mysql-bin.000003", 1),
                                        Arrays.asList(
                                                new MySqlBinlogLagCalculator.BinlogFile(
                                                        "mysql-bin.000001", Long.MAX_VALUE),
                                                new MySqlBinlogLagCalculator.BinlogFile(
                                                        "mysql-bin.000002", 1),
                                                new MySqlBinlogLagCalculator.BinlogFile(
                                                        "mysql-bin.000003", 1))))
                .isInstanceOf(ArithmeticException.class);
    }

    @Test
    void malformedPositionIsReportedToTheCaller() {
        Map<String, String> reference = offset("mysql-bin.000003", 1000);
        reference.put("pos", "not-a-number");

        assertThatThrownBy(
                        () ->
                                MySqlBinlogLagCalculator.calculate(
                                        reference,
                                        offset("mysql-bin.000003", 1250),
                                        Collections.singletonList(
                                                new MySqlBinlogLagCalculator.BinlogFile(
                                                        "mysql-bin.000003", 5000))))
                .isInstanceOf(NumberFormatException.class);
    }

    private static Map<String, String> offset(String file, long position) {
        Map<String, String> offset = new HashMap<>();
        offset.put("file", file);
        offset.put("pos", String.valueOf(position));
        return offset;
    }
}
