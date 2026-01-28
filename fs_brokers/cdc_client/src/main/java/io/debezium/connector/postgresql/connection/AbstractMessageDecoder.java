/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.debezium.connector.postgresql.connection;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;

import io.debezium.connector.postgresql.TypeRegistry;
import io.debezium.connector.postgresql.connection.ReplicationStream.ReplicationMessageProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract implementation of {@link MessageDecoder} that all decoders should inherit from.
 *
 * @author Chris Cranford
 */
public abstract class AbstractMessageDecoder implements MessageDecoder {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractMessageDecoder.class);

    @Override
    public void processMessage(
            ByteBuffer buffer, ReplicationMessageProcessor processor, TypeRegistry typeRegistry)
            throws SQLException, InterruptedException {
        // if message is empty pass control right to ReplicationMessageProcessor to update WAL
        // position info
        if (buffer == null) {
            processor.process(null);
        } else {
            processNotEmptyMessage(buffer, processor, typeRegistry);
        }
    }

    protected abstract void processNotEmptyMessage(
            ByteBuffer buffer, ReplicationMessageProcessor processor, TypeRegistry typeRegistry)
            throws SQLException, InterruptedException;

    @Override
    public boolean shouldMessageBeSkipped(
            ByteBuffer buffer, Lsn lastReceivedLsn, Lsn startLsn, WalPositionLocator walPosition) {
        // the lsn we started from is inclusive, so we need to avoid sending back the same message
        // twice
        // but for the first record seen ever it is possible we received the same LSN as the one
        // obtained from replication slot
        ByteBuffer copy = buffer.asReadOnlyBuffer();
        String str = StandardCharsets.UTF_8.decode(copy).toString();
        if (walPosition.skipMessage(lastReceivedLsn)) {
            LOGGER.info(
                    "Streaming requested from LSN {}, received LSN {} identified as already processed, {}",
                    startLsn,
                    lastReceivedLsn,
                    str);
            return true;
        }
        return false;
    }

    @Override
    public void close() {}
}
