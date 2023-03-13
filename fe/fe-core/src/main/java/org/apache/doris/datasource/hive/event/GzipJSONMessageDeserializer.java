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

package org.apache.doris.datasource.hive.event;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hive.metastore.messaging.AbortTxnMessage;
import org.apache.hadoop.hive.metastore.messaging.AddForeignKeyMessage;
import org.apache.hadoop.hive.metastore.messaging.AddNotNullConstraintMessage;
import org.apache.hadoop.hive.metastore.messaging.AddPartitionMessage;
import org.apache.hadoop.hive.metastore.messaging.AddPrimaryKeyMessage;
import org.apache.hadoop.hive.metastore.messaging.AddUniqueConstraintMessage;
import org.apache.hadoop.hive.metastore.messaging.AllocWriteIdMessage;
import org.apache.hadoop.hive.metastore.messaging.AlterDatabaseMessage;
import org.apache.hadoop.hive.metastore.messaging.AlterPartitionMessage;
import org.apache.hadoop.hive.metastore.messaging.AlterTableMessage;
import org.apache.hadoop.hive.metastore.messaging.CommitTxnMessage;
import org.apache.hadoop.hive.metastore.messaging.CreateDatabaseMessage;
import org.apache.hadoop.hive.metastore.messaging.CreateFunctionMessage;
import org.apache.hadoop.hive.metastore.messaging.CreateTableMessage;
import org.apache.hadoop.hive.metastore.messaging.DropConstraintMessage;
import org.apache.hadoop.hive.metastore.messaging.DropDatabaseMessage;
import org.apache.hadoop.hive.metastore.messaging.DropFunctionMessage;
import org.apache.hadoop.hive.metastore.messaging.DropPartitionMessage;
import org.apache.hadoop.hive.metastore.messaging.DropTableMessage;
import org.apache.hadoop.hive.metastore.messaging.InsertMessage;
import org.apache.hadoop.hive.metastore.messaging.OpenTxnMessage;
import org.apache.hadoop.hive.metastore.messaging.json.JSONMessageDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.zip.GZIPInputStream;

public class GzipJSONMessageDeserializer extends JSONMessageDeserializer {
    private static final Logger LOG = LoggerFactory.getLogger(GzipJSONMessageDeserializer.class.getName());

    public GzipJSONMessageDeserializer() {
    }

    private static String deCompress(String messageBody) {
        try {
            byte[] decodedBytes = Base64.getDecoder().decode(messageBody.getBytes(StandardCharsets.UTF_8));
            String body;
            ByteArrayInputStream in = new ByteArrayInputStream(decodedBytes);
            try {
                GZIPInputStream is = new GZIPInputStream(in);
                try {
                    byte[] bytes = IOUtils.toByteArray(is);
                    body = new String(bytes, StandardCharsets.UTF_8);
                } finally {
                    try {
                        is.close();
                    } catch (Throwable ignore) {
                        LOG.warn("close GZIPInputStream failed", ignore);
                    }
                }
            } finally {
                try {
                    in.close();
                } catch (Throwable ignore) {
                    LOG.warn("close ByteArrayInputStream failed", ignore);
                }
            }
            return body;
        } catch (Exception e) {
            LOG.error("cannot decode the stream", e);
            throw new RuntimeException("cannot decode the stream ", e);
        }
    }

    public CreateDatabaseMessage getCreateDatabaseMessage(String messageBody) {
        return super.getCreateDatabaseMessage(deCompress(messageBody));
    }

    public AlterDatabaseMessage getAlterDatabaseMessage(String messageBody) {
        return super.getAlterDatabaseMessage(deCompress(messageBody));
    }

    public DropDatabaseMessage getDropDatabaseMessage(String messageBody) {
        return super.getDropDatabaseMessage(deCompress(messageBody));
    }

    public CreateTableMessage getCreateTableMessage(String messageBody) {
        return super.getCreateTableMessage(deCompress(messageBody));
    }

    public AlterTableMessage getAlterTableMessage(String messageBody) {
        return super.getAlterTableMessage(deCompress(messageBody));
    }

    public DropTableMessage getDropTableMessage(String messageBody) {
        return super.getDropTableMessage(deCompress(messageBody));
    }

    public AddPartitionMessage getAddPartitionMessage(String messageBody) {
        return super.getAddPartitionMessage(deCompress(messageBody));
    }

    public AlterPartitionMessage getAlterPartitionMessage(String messageBody) {
        return super.getAlterPartitionMessage(deCompress(messageBody));
    }

    public DropPartitionMessage getDropPartitionMessage(String messageBody) {
        return super.getDropPartitionMessage(deCompress(messageBody));
    }

    public CreateFunctionMessage getCreateFunctionMessage(String messageBody) {
        return super.getCreateFunctionMessage(deCompress(messageBody));
    }

    public DropFunctionMessage getDropFunctionMessage(String messageBody) {
        return super.getDropFunctionMessage(deCompress(messageBody));
    }

    public InsertMessage getInsertMessage(String messageBody) {
        return super.getInsertMessage(deCompress(messageBody));
    }

    public AddPrimaryKeyMessage getAddPrimaryKeyMessage(String messageBody) {
        return super.getAddPrimaryKeyMessage(deCompress(messageBody));
    }

    public AddForeignKeyMessage getAddForeignKeyMessage(String messageBody) {
        return super.getAddForeignKeyMessage(deCompress(messageBody));
    }

    public AddUniqueConstraintMessage getAddUniqueConstraintMessage(String messageBody) {
        return super.getAddUniqueConstraintMessage(deCompress(messageBody));
    }

    public AddNotNullConstraintMessage getAddNotNullConstraintMessage(String messageBody) {
        return super.getAddNotNullConstraintMessage(deCompress(messageBody));
    }

    public DropConstraintMessage getDropConstraintMessage(String messageBody) {
        return super.getDropConstraintMessage(deCompress(messageBody));
    }

    public OpenTxnMessage getOpenTxnMessage(String messageBody) {
        return super.getOpenTxnMessage(deCompress(messageBody));
    }

    public CommitTxnMessage getCommitTxnMessage(String messageBody) {
        return super.getCommitTxnMessage(deCompress(messageBody));
    }

    public AbortTxnMessage getAbortTxnMessage(String messageBody) {
        return super.getAbortTxnMessage(deCompress(messageBody));
    }

    public AllocWriteIdMessage getAllocWriteIdMessage(String messageBody) {
        return super.getAllocWriteIdMessage(deCompress(messageBody));
    }

}
