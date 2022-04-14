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

package org.apache.doris.load.sync.canal;

import org.apache.doris.load.sync.model.Events;
import org.apache.doris.load.sync.position.EntryPosition;

import com.alibaba.otter.canal.common.CanalException;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;

import com.google.protobuf.InvalidProtocolBufferException;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.SystemUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public class CanalUtils {
    private static Logger logger = LogManager.getLogger(CanalUtils.class);

    private static final String SEP = SystemUtils.LINE_SEPARATOR;
    private static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";
    private static SimpleDateFormat simpleDateFormat = new SimpleDateFormat(DATE_FORMAT);

    private static String context_format     = null;
    private static String row_format         = null;
    private static String transaction_format = null;

    static {
        context_format = SEP + "----------- Batch Summary ------------------------------>" + SEP;
        context_format += "| Batch Id: [{}] ,count : [{}] , Mem size : [{}] , Time : {}" + SEP;
        context_format += "| Start : [{}] " + SEP;
        context_format += "| End : [{}] " + SEP;
        context_format += "----------------------------------------------------------" + SEP;
        row_format = SEP
                + "----------------> binlog[{}:{}] , name[{},{}] , eventType : {} , executeTime : {}({}) , gtid : ({}) , delay : {} ms"
                + SEP;
        transaction_format = SEP
                + "================> binlog[{}:{}] , executeTime : {}({}) , gtid : ({}) , delay : {}ms"
                + SEP;
    }

    public static void printSummary(Events<CanalEntry.Entry, EntryPosition> dataEvents) {
        List<CanalEntry.Entry> entries = dataEvents.getDatas();
        if (CollectionUtils.isEmpty(entries)) {
            return;
        }
        String startPosition = buildPositionForDump(entries.get(0));
        String endPosition = buildPositionForDump(entries.get(entries.size() - 1));
        SimpleDateFormat format = new SimpleDateFormat(DATE_FORMAT);
        logger.info(context_format, dataEvents.getId(), entries.size(), dataEvents.getMemSize(), format.format(new Date()), startPosition, endPosition);
    }

    public static void printSummary(Message message, int size, long memsize) {
        List<CanalEntry.Entry> entries = message.getEntries();
        if (CollectionUtils.isEmpty(entries)) {
            return;
        }
        String startPosition = buildPositionForDump(message.getEntries().get(0));
        String endPosition = buildPositionForDump(message.getEntries().get(message.getEntries().size() - 1));
        SimpleDateFormat format = new SimpleDateFormat(DATE_FORMAT);
        logger.info(context_format, message.getId(), size, memsize, format.format(new Date()), startPosition, endPosition);
    }

    public static String buildPositionForDump(CanalEntry.Entry entry) {
        CanalEntry.Header header = entry.getHeader();
        long time = entry.getHeader().getExecuteTime();
        Date date = new Date(time);
        SimpleDateFormat format = new SimpleDateFormat(DATE_FORMAT);
        StringBuilder sb = new StringBuilder();
        sb.append(header.getLogfileName())
                .append(":")
                .append(header.getLogfileOffset())
                .append(":")
                .append(header.getExecuteTime())
                .append("(")
                .append(format.format(date))
                .append(")");
        if (StringUtils.isNotEmpty(entry.getHeader().getGtid())) {
            sb.append(" gtid(").append(entry.getHeader().getGtid())
                    .append(")");
        }
        return sb.toString();
    }

    public static String getFullName(String schemaName, String tableName) {
        StringBuilder sb = new StringBuilder();
        if (schemaName != null) {
            sb.append(schemaName).append(".");
        }
        sb.append(tableName);
        return sb.toString().intern();
    }

    public static void printRow(CanalEntry.RowChange rowChange, CanalEntry.Header header) {
        long executeTime = header.getExecuteTime();
        long delayTime = System.currentTimeMillis() - executeTime;
        Date date = new Date(executeTime);
        CanalEntry.EventType eventType = rowChange.getEventType();
        logger.info(row_format, header.getLogfileName(),
                String.valueOf(header.getLogfileOffset()), header.getSchemaName(),
                header.getTableName(), eventType,
                String.valueOf(header.getExecuteTime()), simpleDateFormat.format(date),
                header.getGtid(), String.valueOf(delayTime));
        if (eventType == CanalEntry.EventType.QUERY || rowChange.getIsDdl()) {
            logger.info(" sql ----> " + rowChange.getSql() + SEP);
            return;
        }
        printXAInfo(rowChange.getPropsList());
        for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
            if (eventType == CanalEntry.EventType.DELETE) {
                printColumn(rowData.getBeforeColumnsList());
            } else if (eventType == CanalEntry.EventType.INSERT) {
                printColumn(rowData.getAfterColumnsList());
            } else {
                printColumn(rowData.getAfterColumnsList());
            }
        }
    }

    public static void printColumn(List<CanalEntry.Column> columns) {
        StringBuilder builder = new StringBuilder();
        for (CanalEntry.Column column : columns) {
            try {
                if (StringUtils.containsIgnoreCase(column.getMysqlType(), "BLOB")
                        || StringUtils.containsIgnoreCase(column.getMysqlType(), "BINARY")) {
                    // get value bytes
                    builder.append(column.getName())
                            .append(" : ")
                            .append(new String(column.getValue().getBytes("ISO-8859-1"), "UTF-8"));
                } else {
                    builder.append(column.getName())
                            .append(" : ")
                            .append(column.getValue());
                }
            } catch (UnsupportedEncodingException e) {
            }
            builder.append("    type=").append(column.getMysqlType());
            if (column.getUpdated()) {
                builder.append("    update=").append(column.getUpdated());
            }
            builder.append(SEP);
        }
        logger.info(builder.toString());
    }

    public static void printXAInfo(List<CanalEntry.Pair> pairs) {
        if (pairs == null) {
            return;
        }
        String xaType = null;
        String xaXid = null;
        for (CanalEntry.Pair pair : pairs) {
            String key = pair.getKey();
            if (StringUtils.endsWithIgnoreCase(key, "XA_TYPE")) {
                xaType = pair.getValue();
            } else if (StringUtils.endsWithIgnoreCase(key, "XA_XID")) {
                xaXid = pair.getValue();
            }
        }
        if (xaType != null && xaXid != null) {
            logger.info(" ------> " + xaType + " " + xaXid);
        }
    }

    public static void transactionBegin(CanalEntry.Entry entry) {
        long executeTime = entry.getHeader().getExecuteTime();
        long delayTime = System.currentTimeMillis() - executeTime;
        Date date = new Date(executeTime);
        CanalEntry.TransactionBegin begin = null;
        try {
            begin = CanalEntry.TransactionBegin.parseFrom(entry.getStoreValue());
        } catch (InvalidProtocolBufferException e) {
            throw new CanalException("parse event has an error , data:" + entry.toString(), e);
        }
        // print transaction begin info, thread ID, time consumption
        logger.info(transaction_format,entry.getHeader().getLogfileName(),
                String.valueOf(entry.getHeader().getLogfileOffset()),
                String.valueOf(entry.getHeader().getExecuteTime()), simpleDateFormat.format(date),
                entry.getHeader().getGtid(), String.valueOf(delayTime));
        logger.info(" BEGIN ----> Thread id: {}", begin.getThreadId());
        printXAInfo(begin.getPropsList());
    }

    public static void transactionEnd(CanalEntry.Entry entry) {
        long executeTime = entry.getHeader().getExecuteTime();
        long delayTime = System.currentTimeMillis() - executeTime;
        Date date = new Date(executeTime);
        CanalEntry.TransactionEnd end = null;
        try {
            end = CanalEntry.TransactionEnd.parseFrom(entry.getStoreValue());
        } catch (InvalidProtocolBufferException e) {
            throw new CanalException("parse event has an error , data:" + entry.toString(), e);
        }
        // print transaction end info, transaction ID
        logger.info("----------------\n");
        logger.info(" END ----> transaction id: {}", end.getTransactionId());
        printXAInfo(end.getPropsList());
        logger.info(transaction_format, entry.getHeader().getLogfileName(),
                String.valueOf(entry.getHeader().getLogfileOffset()),
                String.valueOf(entry.getHeader().getExecuteTime()), simpleDateFormat.format(date),
                entry.getHeader().getGtid(), String.valueOf(delayTime));
    }

    public static boolean isDML(CanalEntry.EventType eventType) {
        switch (eventType) {
            case INSERT:
            case UPDATE:
            case DELETE:
                return true;
            default:
                return false;
        }
    }
}