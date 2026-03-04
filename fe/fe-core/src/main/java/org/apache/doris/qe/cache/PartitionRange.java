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

package org.apache.doris.qe.cache;

import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.RangePartitionInfo;
import org.apache.doris.catalog.Type;

import com.google.common.base.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;

/**
 * Convert the range of the partition to the list
 * all partition by day/week/month split to day list
 */
public class PartitionRange {
    private static final Logger LOG = LogManager.getLogger(PartitionRange.class);

    public class PartitionSingle {
        private Partition partition;
        private PartitionKey partitionKey;
        private long partitionId;
        private PartitionKeyType cacheKey;

        public Partition getPartition() {
            return partition;
        }

        public void setPartition(Partition partition) {
            this.partition = partition;
        }

        public PartitionKey getPartitionKey() {
            return partitionKey;
        }

        public void setPartitionKey(PartitionKey key) {
            this.partitionKey = key;
        }

        public long getPartitionId() {
            return partitionId;
        }

        public void setPartitionId(long partitionId) {
            this.partitionId = partitionId;
        }

        public PartitionKeyType getCacheKey() {
            return cacheKey;
        }

        public void setCacheKey(PartitionKeyType cacheKey) {
            this.cacheKey.clone(cacheKey);
        }

        public PartitionSingle() {
            this.partitionId = 0;
            this.cacheKey = new PartitionKeyType();
        }
    }

    public enum KeyType {
        DEFAULT,
        LONG,
        DATE,
        DATETIME,
        TIME
    }

    public static class PartitionKeyType {
        private DateTimeFormatter df8 = DateTimeFormatter.ofPattern("yyyyMMdd").withZone(ZoneId.systemDefault());
        private DateTimeFormatter df10 = DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneId.systemDefault());

        public KeyType keyType = KeyType.DEFAULT;
        public long value;
        public Date date;

        public boolean init(Type type, String str) {
            switch (type.getPrimitiveType()) {
                case DATE:
                case DATEV2:
                    try {
                        date = Date.from(
                                LocalDate.parse(str, df10).atStartOfDay().atZone(ZoneId.systemDefault()).toInstant());
                    } catch (Exception e) {
                        LOG.warn("parse error str{}.", str);
                        return false;
                    }
                    keyType = KeyType.DATE;
                    break;
                case TINYINT:
                case SMALLINT:
                case INT:
                case BIGINT:
                    value = Long.parseLong(str);
                    keyType = KeyType.LONG;
                    break;
                default:
                    LOG.info("PartitionCache not support such key type {}", type.toSql());
                    return false;
            }
            return true;
        }

        public boolean init(Type type, LiteralExpr expr) {
            switch (type.getPrimitiveType()) {
                case BOOLEAN:
                case TIMEV2:
                case DATETIME:
                case DATETIMEV2:
                case TIMESTAMPTZ:
                case FLOAT:
                case DOUBLE:
                case DECIMALV2:
                case DECIMAL32:
                case DECIMAL64:
                case DECIMAL128:
                case DECIMAL256:
                case CHAR:
                case VARCHAR:
                case STRING:
                case LARGEINT:
                    LOG.info("PartitionCache not support such key type {}", type.toSql());
                    return false;
                case DATE:
                case DATEV2:
                    date = getDateValue(expr);
                    keyType = KeyType.DATE;
                    break;
                case TINYINT:
                case SMALLINT:
                case INT:
                case BIGINT:
                    value = expr.getLongValue();
                    keyType = KeyType.LONG;
                    break;
                default:
                    return true;
            }
            return true;
        }

        public void clone(PartitionKeyType key) {
            keyType = key.keyType;
            value = key.value;
            date = key.date;
        }

        public void add(int num) {
            if (keyType == KeyType.DATE) {
                date = new Date(date.getTime() + num * 3600 * 24 * 1000);
            } else {
                value += num;
            }
        }

        public String toString() {
            if (keyType == KeyType.DEFAULT) {
                return "";
            } else if (keyType == KeyType.DATE) {
                return df10.format(LocalDateTime.ofInstant(date.toInstant(), ZoneId.systemDefault()));
            } else {
                return String.valueOf(value);
            }
        }

        public long realValue() {
            if (keyType == KeyType.DATE) {
                return Long.parseLong(df8.format(LocalDateTime.ofInstant(date.toInstant(), ZoneId.systemDefault())));
            } else {
                return value;
            }
        }

        private Date getDateValue(LiteralExpr expr) {
            Preconditions.checkArgument(expr.getType() == Type.DATE || expr.getType() == Type.DATEV2);
            value = expr.getLongValue();
            Date dt = null;
            try {
                dt = Date.from(LocalDate.parse(String.valueOf(value), df8).atStartOfDay().atZone(ZoneId.systemDefault())
                        .toInstant());
            } catch (Exception e) {
                // CHECKSTYLE IGNORE THIS LINE
            }
            return dt;
        }
    }

    private RangePartitionInfo rangePartitionInfo;
    private Column partitionColumn;

    public RangePartitionInfo getRangePartitionInfo() {
        return rangePartitionInfo;
    }

    public void setRangePartitionInfo(RangePartitionInfo rangePartitionInfo) {
        this.rangePartitionInfo = rangePartitionInfo;
    }

    public Column getPartitionColumn() {
        return partitionColumn;
    }

    public void setPartitionColumn(Column partitionColumn) {
        this.partitionColumn = partitionColumn;
    }

    public PartitionRange() {
    }
}
