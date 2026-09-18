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
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.FeConstants;
import org.apache.doris.mysql.FieldInfo;
import org.apache.doris.mysql.MysqlChannel;
import org.apache.doris.mysql.MysqlCommand;
import org.apache.doris.mysql.MysqlEofPacket;
import org.apache.doris.mysql.MysqlResultSetEndPacket;
import org.apache.doris.mysql.MysqlSerializer;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ResultSet;
import org.apache.doris.qe.ResultSetMetaData;
import org.apache.doris.qe.ShortCircuitQueryContext;
import org.apache.doris.qe.protocol.ResultSender;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * Encodes results as MySQL protocol packets and writes them to the connection's
 * {@link MysqlChannel}. The packets of a result set are: a column count, one column definition
 * per column, a terminator after the definitions unless the client deprecated it, then one packet
 * per row in text or binary format. The packet that ends the result set is the statement's
 * response and is sent by {@link MysqlProtocolAdapter#finishCommand}.
 *
 * <p>The serializer is the channel's, so a sender obtained from a session's adapter can be handed
 * to an executor of another (internal) session: what that executor produces then reaches this
 * session's client, encoded with this session's negotiated capabilities.
 */
public class MysqlResultSender implements ResultSender {
    private static final Logger LOG = LogManager.getLogger(MysqlResultSender.class);

    private final ConnectContext ctx;
    private final MysqlProtocolAdapter adapter;

    MysqlResultSender(ConnectContext ctx, MysqlProtocolAdapter adapter) {
        this.ctx = ctx;
        this.adapter = adapter;
    }

    private MysqlChannel channel() {
        return adapter.getChannel();
    }

    private MysqlSerializer serializer() {
        return adapter.getChannel().getSerializer();
    }

    @Override
    public void sendResultSet(ResultSet resultSet, List<FieldInfo> fieldInfos, boolean binaryRows)
            throws IOException {
        sendMetaData(resultSet.getMetaData(), fieldInfos);
        if (binaryRows) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Use binary protocol to set result.");
            }
            sendBinaryResultRow(resultSet);
        } else {
            sendTextResultRow(resultSet);
        }
    }

    @Override
    public void sendFields(List<String> colNames, List<FieldInfo> fieldInfos, List<Type> types) throws IOException {
        MysqlSerializer serializer = serializer();
        MysqlChannel channel = channel();
        // sends how many columns
        serializer.reset();
        serializer.writeVInt(colNames.size());
        if (LOG.isDebugEnabled()) {
            LOG.debug("sendFields {}", colNames);
        }
        channel.sendOnePacket(serializer.toByteBuffer());
        StatementContext statementContext = ctx.getStatementContext();
        boolean isShortCircuited = statementContext.isShortCircuitQuery()
                && statementContext.getShortCircuitQueryContext() != null;
        ShortCircuitQueryContext shortCircuitCtx = statementContext.getShortCircuitQueryContext();
        // send field one by one
        for (int i = 0; i < colNames.size(); ++i) {
            serializer.reset();
            if (ctx.getCommand() == MysqlCommand.COM_STMT_EXECUTE && isShortCircuited) {
                // Using PreparedStatment pre serializedField to avoid serialize each time
                // we send a field
                byte[] serializedField = shortCircuitCtx.getSerializedField(i);
                if (serializedField == null) {
                    if (fieldInfos != null) {
                        serializer.writeField(fieldInfos.get(i), types.get(i));
                    } else {
                        serializer.writeField(colNames.get(i), types.get(i));
                    }
                    serializedField = serializer.toArray();
                    shortCircuitCtx.addSerializedField(i, serializedField);
                }
                channel.sendOnePacket(ByteBuffer.wrap(serializedField));
            } else {
                if (fieldInfos != null) {
                    serializer.writeField(fieldInfos.get(i), types.get(i));
                } else {
                    serializer.writeField(colNames.get(i), types.get(i));
                }
                channel.sendOnePacket(serializer.toByteBuffer());
            }
        }
        sendMetadataTerminatorIfNeeded();
    }

    @Override
    public void sendRow(ByteBuffer row) throws IOException {
        channel().sendOnePacket(row);
    }

    /** Clears the send flag and whatever a failed attempt of the query left in the send buffer. */
    @Override
    public void reset() {
        channel().reset();
    }

    /**
     * The response to COM_STMT_PREPARE: the OK packet with the statement id, then the parameter
     * definitions and the column definitions, each list followed by an EOF unless the client
     * deprecated it. Flushes, there is no separate terminator.
     */
    public void sendStmtPrepareOK(int stmtId, List<String> labels, List<Slot> output) throws IOException {
        MysqlSerializer serializer = serializer();
        MysqlChannel channel = channel();
        // https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_prepare.html#sect_protocol_com_stmt_prepare_response
        serializer.reset();
        // 0x00 OK
        serializer.writeInt1(0);
        // statement_id
        serializer.writeInt4(stmtId);
        // num_columns
        int numColumns = output == null ? 0 : output.size();
        serializer.writeInt2(numColumns);
        // num_params
        int numParams = labels.size();
        serializer.writeInt2(numParams);
        // reserved_1
        serializer.writeInt1(0);
        if (numParams > 0 || numColumns > 0) {
            // warning_count
            serializer.writeInt2(0);
            // metadata_follows
            serializer.writeInt1(1);
        }
        channel.sendOnePacket(serializer.toByteBuffer());
        if (numParams > 0) {
            // send field one by one
            // TODO use real type instead of string, for JDBC client it's ok
            // but for other client, type should be correct
            // List<PrimitiveType> types = exprToStringType(labels);
            List<String> colNames = labels;
            for (int i = 0; i < colNames.size(); ++i) {
                serializer.reset();
                // serializer.writeField(colNames.get(i), Type.fromPrimitiveType(types.get(i)));
                serializer.writeField(colNames.get(i), Type.STRING);
                channel.sendOnePacket(serializer.toByteBuffer());
            }
            // When CLIENT_DEPRECATE_EOF is set, no EOF/OK packet should be sent after
            // parameter definitions. The driver knows how many params to expect from the
            // prepare OK packet and simply stops reading after that count.
            if (!channel.clientDeprecatedEOF()) {
                serializer.reset();
                MysqlEofPacket eofPacket = new MysqlEofPacket(ctx.getState());
                eofPacket.writeTo(serializer);
                channel.sendOnePacket(serializer.toByteBuffer());
            }
        }
        if (numColumns > 0) {
            for (Slot slot : output) {
                serializer.reset();
                if (slot instanceof SlotReference
                        && ((SlotReference) slot).getOriginalColumn().isPresent()
                        && ((SlotReference) slot).getOriginalTable().isPresent()) {
                    SlotReference slotReference = (SlotReference) slot;
                    TableIf table = slotReference.getOriginalTable().get();
                    Column column = slotReference.getOriginalColumn().get();
                    DatabaseIf database = table.getDatabase();
                    String dbName = database == null ? "" : database.getFullName();
                    serializer.writeField(dbName, table.getName(), column, false);
                } else {
                    serializer.writeField(slot.getName(), slot.getDataType().toCatalogDataType());
                }
                channel.sendOnePacket(serializer.toByteBuffer());
            }
            // When CLIENT_DEPRECATE_EOF is set, no EOF/OK packet should be sent after
            // column definitions. The driver knows how many columns to expect from the
            // prepare OK packet and simply stops reading after that count.
            if (!channel.clientDeprecatedEOF()) {
                serializer.reset();
                MysqlEofPacket eofPacket = new MysqlEofPacket(ctx.getState());
                eofPacket.writeTo(serializer);
                channel.sendOnePacket(serializer.toByteBuffer());
            }
        }
        channel.flush();
    }

    /**
     * The response to COM_FIELD_LIST: one column definition per column of the table, with the
     * default values, and no column count. The terminator is the command's response.
     */
    public void sendFieldList(String dbName, String tableName, List<Column> columns) throws IOException {
        MysqlSerializer serializer = serializer();
        MysqlChannel channel = channel();
        for (Column column : columns) {
            serializer.reset();
            serializer.writeField(dbName, tableName, column, true);
            channel.sendOnePacket(serializer.toByteBuffer());
        }
    }

    private void sendMetaData(ResultSetMetaData metaData, List<FieldInfo> fieldInfos) throws IOException {
        MysqlSerializer serializer = serializer();
        MysqlChannel channel = channel();
        // sends how many columns
        serializer.reset();
        serializer.writeVInt(metaData.getColumnCount());
        channel.sendOnePacket(serializer.toByteBuffer());
        // send field one by one
        for (int i = 0; i < metaData.getColumns().size(); i++) {
            Column col = metaData.getColumn(i);
            serializer.reset();
            if (fieldInfos == null) {
                // TODO(zhaochun): only support varchar type
                serializer.writeField(col.getName(), col.getType());
            } else {
                serializer.writeField(fieldInfos.get(i), col.getType());
            }
            channel.sendOnePacket(serializer.toByteBuffer());
        }
        sendMetadataTerminatorIfNeeded();
    }

    private void sendMetadataTerminatorIfNeeded() throws IOException {
        MysqlSerializer serializer = serializer();
        MysqlChannel channel = channel();
        if (!channel.clientDeprecatedEOF()) {
            serializer.reset();
            new MysqlEofPacket(ctx.getState()).writeTo(serializer);
            channel.sendOnePacket(serializer.toByteBuffer());
        } else if (adapter.clientConsumesCursorMetadataTerminator(ctx)) {
            // Connector/J before 9.5 consumes the first OK packet after column definitions
            // while probing whether a requested cursor was created. Doris does not create a
            // cursor, so an empty result would otherwise lose its only end marker and block.
            serializer.reset();
            new MysqlResultSetEndPacket(ctx.getState()).writeTo(serializer);
            channel.sendOnePacket(serializer.toByteBuffer());
        }
    }

    private void sendTextResultRow(ResultSet resultSet) throws IOException {
        MysqlSerializer serializer = serializer();
        MysqlChannel channel = channel();
        for (List<String> row : resultSet.getResultRows()) {
            serializer.reset();
            for (String item : row) {
                if (item == null || item.equals(FeConstants.null_string)) {
                    serializer.writeNull();
                } else {
                    serializer.writeLenEncodedString(item);
                }
            }
            channel.sendOnePacket(serializer.toByteBuffer());
        }
    }

    private void sendBinaryResultRow(ResultSet resultSet) throws IOException {
        MysqlSerializer serializer = serializer();
        MysqlChannel channel = channel();
        // https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_binary_resultset.html#sect_protocol_binary_resultset_row_value
        ResultSetMetaData metaData = resultSet.getMetaData();
        int nullBitmapLength = (metaData.getColumnCount() + 7 + 2) / 8;
        for (List<String> row : resultSet.getResultRows()) {
            serializer.reset();
            // Reserved one byte.
            serializer.writeByte((byte) 0x00);
            byte[] nullBitmap = new byte[nullBitmapLength];
            // Generate null bitmap
            for (int i = 0; i < row.size(); i++) {
                String item = row.get(i);
                if (item == null || item.equals(FeConstants.null_string)) {
                    // The first 2 bits are reserved.
                    int byteIndex = (i + 2) / 8;  // Index of the byte in the bitmap array
                    int bitInByte = (i + 2) % 8;  // Position within the target byte (0-7)
                    nullBitmap[byteIndex] |= (1 << bitInByte);
                }
            }
            // Null bitmap
            serializer.writeBytes(nullBitmap);
            // Non-null columns
            for (int i = 0; i < row.size(); i++) {
                String item = row.get(i);
                if (item != null && !item.equals(FeConstants.null_string)) {
                    Column col = metaData.getColumn(i);
                    switch (col.getType().getPrimitiveType()) {
                        case BOOLEAN:
                            serializer.writeInt1(parseBooleanResultValue(item));
                            break;
                        case INT:
                            serializer.writeInt4(Integer.parseInt(item));
                            break;
                        case BIGINT:
                            serializer.writeInt8(Long.parseLong(item));
                            break;
                        case DATETIME:
                        case DATETIMEV2:
                            DateTimeV2Literal datetime = new DateTimeV2Literal(item);
                            long microSecond = datetime.getMicroSecond();
                            // https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response_text_resultset.html
                            int length = microSecond == 0 ? 7 : 11;
                            serializer.writeInt1(length);
                            serializer.writeInt2((int) (datetime.getYear()));
                            serializer.writeInt1((int) datetime.getMonth());
                            serializer.writeInt1((int) datetime.getDay());
                            serializer.writeInt1((int) datetime.getHour());
                            serializer.writeInt1((int) datetime.getMinute());
                            serializer.writeInt1((int) datetime.getSecond());
                            if (microSecond > 0) {
                                serializer.writeInt4((int) microSecond);
                            }
                            break;
                        case TIMESTAMP_NS:
                            // MySQL temporal binary values cannot carry nanoseconds. The metadata advertises
                            // MYSQL_TYPE_STRING, so encode the result as length-encoded text.
                            serializer.writeLenEncodedString(item);
                            break;
                        default:
                            serializer.writeLenEncodedString(item);
                    }
                }
            }
            channel.sendOnePacket(serializer.toByteBuffer());
        }
    }

    private static int parseBooleanResultValue(String item) {
        if ("1".equals(item) || "true".equalsIgnoreCase(item)) {
            return 1;
        }
        if ("0".equals(item) || "false".equalsIgnoreCase(item)) {
            return 0;
        }
        throw new IllegalArgumentException("Invalid boolean result value: " + item);
    }
}
