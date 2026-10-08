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

package org.apache.doris.mysql.protocol;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.FeConstants;
import org.apache.doris.mysql.MysqlChannel;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.qe.CommonResultSet;
import org.apache.doris.qe.CommonResultSet.CommonResultSetMetaData;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ResultSet;
import org.apache.doris.qe.protocol.RecordingMysqlChannel;
import org.apache.doris.qe.protocol.RecordingMysqlChannel.RecordedPacket;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * The packets a MysqlResultSender writes for a result set, a prepared statement and a raw row.
 * The end-to-end packet sequences of whole commands are in MysqlPacketGoldenTest; this checks
 * the encoding of the pieces.
 */
public class MysqlResultSenderTest {
    private boolean savedRunningUnitTest;

    @BeforeEach
    public void setUp() {
        savedRunningUnitTest = FeConstants.runningUnitTest;
        // ConnectContext.init() registers the session with Env unless running as a unit test.
        FeConstants.runningUnitTest = true;
    }

    @AfterEach
    public void tearDown() {
        FeConstants.runningUnitTest = savedRunningUnitTest;
    }

    @Test
    public void testTextRowsEncodeNullAndLengthEncodedStrings() throws IOException {
        RecordingMysqlChannel channel = newChannel(true);
        ConnectContext ctx = newContext(channel);
        List<Column> columns = Lists.newArrayList(
                new Column("c1", PrimitiveType.VARCHAR), new Column("c2", PrimitiveType.VARCHAR));
        List<List<String>> rows = Lists.newArrayList();
        rows.add(Lists.newArrayList(null, "row1"));
        rows.add(Lists.newArrayList("1234", "row2"));
        ResultSet resultSet = new CommonResultSet(new CommonResultSetMetaData(columns), rows);

        ctx.getResultSender().sendResultSet(resultSet, null, false);

        // column count, two column definitions, no terminator (CLIENT_DEPRECATE_EOF), two rows
        List<byte[]> packets = payloads(channel);
        Assertions.assertEquals(5, packets.size());
        Assertions.assertArrayEquals(new byte[] {2}, packets.get(0));
        Assertions.assertArrayEquals(new byte[] {-5, 4, 114, 111, 119, 49}, packets.get(3));
        Assertions.assertArrayEquals(new byte[] {4, 49, 50, 51, 52, 4, 114, 111, 119, 50}, packets.get(4));
    }

    @Test
    public void testBinaryRowsEncodeIntegersAndDatetimes() throws IOException {
        RecordingMysqlChannel channel = newChannel(true);
        ConnectContext ctx = newContext(channel);
        List<Column> columns = Lists.newArrayList(
                new Column("col1", PrimitiveType.BIGINT), new Column("col2", PrimitiveType.DATETIMEV2));
        List<List<String>> rows = Lists.newArrayList();
        rows.add(Lists.newArrayList(null, "2025-01-01 01:02:03"));
        rows.add(Lists.newArrayList("1234", "2025-01-01 01:02:03.123456"));
        ResultSet resultSet = new CommonResultSet(new CommonResultSetMetaData(columns), rows);

        ctx.getResultSender().sendResultSet(resultSet, null, true);

        List<byte[]> packets = payloads(channel);
        Assertions.assertEquals(5, packets.size());
        // header byte, null bitmap (first column null), 7-byte datetime without microseconds
        Assertions.assertArrayEquals(new byte[] {0, 4, 7, -23, 7, 1, 1, 1, 2, 3}, packets.get(3));
        // header byte, empty null bitmap, int8, 11-byte datetime with microseconds
        Assertions.assertArrayEquals(new byte[] {0, 0, -46, 4, 0, 0, 0, 0, 0, 0, 11, -23, 7, 1, 1, 1, 2, 3,
                64, -30, 1, 0}, packets.get(4));
    }

    @Test
    public void testBinaryTimestampNsIsSentAsText() throws IOException {
        RecordingMysqlChannel channel = newChannel(true);
        ConnectContext ctx = newContext(channel);
        String value = "2025-01-01 01:02:03.123456789";
        List<List<String>> rows = Lists.newArrayList();
        rows.add(Lists.newArrayList(value));
        ResultSet resultSet = new CommonResultSet(
                new CommonResultSetMetaData(Lists.newArrayList(
                        new Column("timestamp_ns", ScalarType.createTimeStampNsType()))),
                rows);

        ctx.getResultSender().sendResultSet(resultSet, null, true);

        byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);
        byte[] expected = new byte[valueBytes.length + 3];
        expected[2] = (byte) valueBytes.length;
        System.arraycopy(valueBytes, 0, expected, 3, valueBytes.length);
        List<byte[]> packets = payloads(channel);
        Assertions.assertArrayEquals(expected, packets.get(packets.size() - 1));
    }

    @Test
    public void testBinaryBooleanAcceptsBothSpellings() throws IOException {
        RecordingMysqlChannel channel = newChannel(true);
        ConnectContext ctx = newContext(channel);
        List<List<String>> rows = Lists.newArrayList();
        rows.add(Lists.newArrayList("false"));
        rows.add(Lists.newArrayList("1"));
        ResultSet resultSet = new CommonResultSet(
                new CommonResultSetMetaData(Lists.newArrayList(new Column("col1", PrimitiveType.BOOLEAN))),
                rows);

        ctx.getResultSender().sendResultSet(resultSet, null, true);

        List<byte[]> packets = payloads(channel);
        Assertions.assertArrayEquals(new byte[] {0, 0, 0}, packets.get(packets.size() - 2));
        Assertions.assertArrayEquals(new byte[] {0, 0, 1}, packets.get(packets.size() - 1));
    }

    @Test
    public void testCursorFetchMetadataTerminatorDependsOnConnectorJVersion() throws IOException {
        List<byte[]> connector82Packets = sendEmptyResultSet(true, "MySQL Connector/J", "8.2.0");
        Assertions.assertEquals(3, connector82Packets.size());
        Assertions.assertEquals(0xFE, Byte.toUnsignedInt(connector82Packets.get(2)[0]));
        Assertions.assertTrue(connector82Packets.get(2).length > 5);

        Assertions.assertEquals(3, sendEmptyResultSet(true, "MySQL Connector Java", "5.1.49").size());
        Assertions.assertEquals(3, sendEmptyResultSet(true, "MySQL Connector/J", "6.0.6").size());
        Assertions.assertEquals(3, sendEmptyResultSet(true, "MySQL Connector/J", "9.4.0").size());
        Assertions.assertEquals(2, sendEmptyResultSet(true, "MySQL Connector/J", "9.5.0").size());
        Assertions.assertEquals(2, sendEmptyResultSet(false, "MySQL Connector/J", "8.2.0").size());
        Assertions.assertEquals(2, sendEmptyResultSet(true, "MariaDB Connector/J", "3.5.6").size());
        Assertions.assertEquals(3, sendEmptyResultSet(true, Collections.emptyMap(), true).size());

        List<byte[]> legacyEofPackets = sendEmptyResultSet(true, "MySQL Connector/J", "8.2.0", false);
        Assertions.assertEquals(3, legacyEofPackets.size());
        Assertions.assertEquals(5, legacyEofPackets.get(2).length);
    }

    @Test
    public void testPrepareMetadataTerminatorsFollowNegotiatedCapability() throws IOException {
        Assertions.assertEquals(3, sendPrepareMetadata(false).size());
        Assertions.assertEquals(2, sendPrepareMetadata(true).size());
    }

    @Test
    public void testPrepareResponseIsFlushedWithItsLastPacket() throws IOException {
        RecordingMysqlChannel channel = newChannel(true);
        ConnectContext ctx = newContext(channel);

        MysqlProtocolAdapter.of(ctx).resultSender(ctx).sendStmtPrepareOK(
                7, Collections.singletonList("p"), Collections.emptyList());

        List<RecordedPacket> packets = channel.getOutbound();
        // statement id 7, one parameter, no columns
        Assertions.assertArrayEquals(new byte[] {0, 7, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1}, packets.get(0).getPayload());
        Assertions.assertTrue(packets.get(packets.size() - 1).isFlushed());
    }

    @Test
    public void testBackendRowsAreWrittenAsIs() throws IOException {
        RecordingMysqlChannel channel = newChannel(true);
        ConnectContext ctx = newContext(channel);
        ctx.setStatementContext(new StatementContext());

        ctx.getResultSender().sendFields(Lists.newArrayList("k1"), null, Lists.newArrayList(Type.INT));
        ctx.getResultSender().sendRow(ByteBuffer.wrap(new byte[] {1, 49}));

        List<byte[]> packets = payloads(channel);
        // column count, one column definition, the row untouched
        Assertions.assertEquals(3, packets.size());
        Assertions.assertArrayEquals(new byte[] {1}, packets.get(0));
        Assertions.assertArrayEquals(new byte[] {1, 49}, packets.get(2));
    }

    @Test
    public void testFieldListHasNoColumnCount() throws IOException {
        RecordingMysqlChannel channel = newChannel(true);
        ConnectContext ctx = newContext(channel);
        List<Column> columns = Lists.newArrayList(
                new Column("k1", PrimitiveType.INT), new Column("k2", PrimitiveType.VARCHAR));

        MysqlProtocolAdapter.of(ctx).resultSender(ctx).sendFieldList("db1", "tbl1", columns);

        Assertions.assertEquals(2, channel.getOutbound().size());
    }

    @Test
    public void testResetClearsTheChannel() {
        MysqlChannel channel = Mockito.mock(MysqlChannel.class);
        ConnectContext ctx = new ConnectContext(new MysqlProtocolAdapter(channel));

        ctx.getResultSender().reset();

        Mockito.verify(channel).reset();
    }

    private List<byte[]> sendPrepareMetadata(boolean clientDeprecatedEof) throws IOException {
        RecordingMysqlChannel channel = newChannel(clientDeprecatedEof);
        ConnectContext ctx = newContext(channel);

        MysqlProtocolAdapter.of(ctx).resultSender(ctx).sendStmtPrepareOK(
                1, Collections.singletonList("p"), Collections.emptyList());
        return payloads(channel);
    }

    private List<byte[]> sendEmptyResultSet(boolean cursorFetchRequested, String clientName,
            String clientVersion) throws IOException {
        return sendEmptyResultSet(cursorFetchRequested, clientName, clientVersion, true);
    }

    private List<byte[]> sendEmptyResultSet(boolean cursorFetchRequested, String clientName,
            String clientVersion, boolean clientDeprecatedEof) throws IOException {
        return sendEmptyResultSet(cursorFetchRequested, ImmutableMap.of(
                "_client_name", clientName, "_client_version", clientVersion), clientDeprecatedEof);
    }

    private List<byte[]> sendEmptyResultSet(boolean cursorFetchRequested,
            Map<String, String> connectAttributes, boolean clientDeprecatedEof) throws IOException {
        RecordingMysqlChannel channel = newChannel(clientDeprecatedEof);
        ConnectContext ctx = newContext(channel);
        ctx.setConnectAttributes(connectAttributes);
        MysqlProtocolAdapter.of(ctx).setCursorFetchRequested(cursorFetchRequested);

        List<Column> columns = Collections.singletonList(new Column("c", PrimitiveType.INT));
        ResultSet resultSet = new CommonResultSet(new CommonResultSetMetaData(columns), Collections.emptyList());
        ctx.getResultSender().sendResultSet(resultSet, null, true);
        return payloads(channel);
    }

    private static RecordingMysqlChannel newChannel(boolean clientDeprecatedEof) {
        RecordingMysqlChannel channel = new RecordingMysqlChannel();
        if (clientDeprecatedEof) {
            channel.setClientDeprecatedEOF();
        }
        return channel;
    }

    private static ConnectContext newContext(RecordingMysqlChannel channel) {
        return new ConnectContext(new MysqlProtocolAdapter(channel));
    }

    private static List<byte[]> payloads(RecordingMysqlChannel channel) {
        return channel.getOutbound().stream().map(RecordedPacket::getPayload).collect(Collectors.toList());
    }
}
