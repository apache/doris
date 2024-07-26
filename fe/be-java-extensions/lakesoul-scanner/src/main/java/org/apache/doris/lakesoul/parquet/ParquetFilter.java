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

package org.apache.doris.lakesoul.parquet;

import org.apache.doris.common.jni.vec.ColumnType;
import org.apache.doris.common.jni.vec.ScanPredicate;

import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.io.api.Binary;

public class ParquetFilter {

    public static FilterPredicate toParquetFilter(ScanPredicate predicate) {
        ScanPredicate.FilterOp filterOp = predicate.op;
        switch (filterOp) {
            case FILTER_IN:
                return convertIn(predicate);
            case FILTER_NOT_IN:
                return convertNotIn(predicate);
            case FILTER_LESS:
                return convertLess(predicate);
            case FILTER_LARGER:
                return convertLarger(predicate);
            case FILTER_LESS_OR_EQUAL:
                return convertLessOrEqual(predicate);
            case FILTER_LARGER_OR_EQUAL:
                return convertLargerOrEqual(predicate);
            default:
                break;
        }
        throw new RuntimeException("Unsupported ScanPredicate" + ScanPredicate.dump(new ScanPredicate[] {predicate}));
    }

    private static FilterPredicate convertNotIn(ScanPredicate predicate) {
        String colName = predicate.columName;
        ColumnType.Type colType = predicate.type;
        ScanPredicate.PredicateValue[] predicateValues = predicate.predicateValues();
        FilterPredicate resultPredicate = null;
        for (ScanPredicate.PredicateValue predicateValue : predicateValues) {
            if (resultPredicate == null) {
                resultPredicate = makeNotEquals(colName, colType, predicateValue);
            } else {
                resultPredicate = FilterApi.and(resultPredicate, makeNotEquals(colName, colType, predicateValue));
            }
        }
        return resultPredicate;
    }

    private static FilterPredicate convertIn(ScanPredicate predicate) {
        String colName = predicate.columName;
        ColumnType.Type colType = predicate.type;
        ScanPredicate.PredicateValue[] predicateValues = predicate.predicateValues();
        FilterPredicate resultPredicate = null;
        for (ScanPredicate.PredicateValue predicateValue : predicateValues) {
            if (resultPredicate == null) {
                resultPredicate = makeEquals(colName, colType, predicateValue);
            } else {
                resultPredicate = FilterApi.or(resultPredicate, makeEquals(colName, colType, predicateValue));
            }
        }
        return resultPredicate;
    }

    private static FilterPredicate convertLarger(ScanPredicate predicate) {
        String colName = predicate.columName;
        ColumnType.Type colType = predicate.type;
        ScanPredicate.PredicateValue predicateValue = predicate.predicateValues()[0];
        return makeLarger(colName, colType, predicateValue);
    }

    private static FilterPredicate convertLargerOrEqual(ScanPredicate predicate) {
        String colName = predicate.columName;
        ColumnType.Type colType = predicate.type;
        ScanPredicate.PredicateValue predicateValue = predicate.predicateValues()[0];
        return makeLargerOrEqual(colName, colType, predicateValue);
    }

    private static FilterPredicate convertLess(ScanPredicate predicate) {
        String colName = predicate.columName;
        ColumnType.Type colType = predicate.type;
        ScanPredicate.PredicateValue predicateValue = predicate.predicateValues()[0];
        return makeLess(colName, colType, predicateValue);
    }

    private static FilterPredicate convertLessOrEqual(ScanPredicate predicate) {
        String colName = predicate.columName;
        ColumnType.Type colType = predicate.type;
        ScanPredicate.PredicateValue predicateValue = predicate.predicateValues()[0];
        return makeLessOrEqual(colName, colType, predicateValue);
    }

    private static FilterPredicate makeNotEquals(String colName, ColumnType.Type type,
                                                 ScanPredicate.PredicateValue value) {
        switch (type) {
            case BOOLEAN:
                return FilterApi.notEq(FilterApi.booleanColumn(colName), value.getBoolean());
            case TINYINT:
                return FilterApi.notEq(FilterApi.intColumn(colName), (int) value.getByte());
            case SMALLINT:
                return FilterApi.notEq(FilterApi.intColumn(colName), (int) value.getShort());
            case INT:
                return FilterApi.notEq(FilterApi.intColumn(colName), value.getInt());
            case BIGINT:
                return FilterApi.notEq(FilterApi.longColumn(colName), value.getLong());
            case FLOAT:
                return FilterApi.notEq(FilterApi.floatColumn(colName), value.getFloat());
            case DOUBLE:
                return FilterApi.notEq(FilterApi.doubleColumn(colName), value.getDouble());
            case CHAR:
            case VARCHAR:
            case STRING:
                return FilterApi.notEq(FilterApi.binaryColumn(colName), Binary.fromString(value.getString()));
            case BINARY:
                return FilterApi.notEq(FilterApi.binaryColumn(colName), Binary.fromConstantByteArray(value.getBytes()));
            case ARRAY:
            case MAP:
            case STRUCT:
            default:
                throw new RuntimeException("Unsupported push_down_filter type value: " + type);
        }
    }


    private static FilterPredicate makeEquals(String colName, ColumnType.Type type,
                                              ScanPredicate.PredicateValue value) {
        switch (type) {
            case BOOLEAN:
                return FilterApi.eq(FilterApi.booleanColumn(colName), value.getBoolean());
            case TINYINT:
                return FilterApi.eq(FilterApi.intColumn(colName), (int) value.getByte());
            case SMALLINT:
                return FilterApi.eq(FilterApi.intColumn(colName), (int) value.getShort());
            case INT:
                return FilterApi.eq(FilterApi.intColumn(colName), value.getInt());
            case BIGINT:
                return FilterApi.eq(FilterApi.longColumn(colName), value.getLong());
            case FLOAT:
                return FilterApi.eq(FilterApi.floatColumn(colName), value.getFloat());
            case DOUBLE:
                return FilterApi.eq(FilterApi.doubleColumn(colName), value.getDouble());
            case CHAR:
            case VARCHAR:
            case STRING:
                return FilterApi.eq(FilterApi.binaryColumn(colName), Binary.fromString(value.getString()));
            case BINARY:
                return FilterApi.eq(FilterApi.binaryColumn(colName), Binary.fromConstantByteArray(value.getBytes()));
            case ARRAY:
            case MAP:
            case STRUCT:
            default:
                throw new RuntimeException("Unsupported push_down_filter type value: " + type);
        }
    }

    private static FilterPredicate makeLarger(String colName, ColumnType.Type type,
                                              ScanPredicate.PredicateValue value) {
        switch (type) {
            case TINYINT:
                return FilterApi.gt(FilterApi.intColumn(colName), (int) value.getByte());
            case SMALLINT:
                return FilterApi.gt(FilterApi.intColumn(colName), (int) value.getShort());
            case INT:
                return FilterApi.gt(FilterApi.intColumn(colName), value.getInt());
            case BIGINT:
                return FilterApi.gt(FilterApi.longColumn(colName), value.getLong());
            case FLOAT:
                return FilterApi.gt(FilterApi.floatColumn(colName), value.getFloat());
            case DOUBLE:
                return FilterApi.gt(FilterApi.doubleColumn(colName), value.getDouble());
            case CHAR:
            case VARCHAR:
            case STRING:
                return FilterApi.gt(FilterApi.binaryColumn(colName), Binary.fromString(value.getString()));
            case BINARY:
                return FilterApi.gt(FilterApi.binaryColumn(colName), Binary.fromConstantByteArray(value.getBytes()));
            case ARRAY:
            case MAP:
            case STRUCT:
            default:
                throw new RuntimeException("Unsupported push_down_filter type value: " + type);
        }

    }

    private static FilterPredicate makeLargerOrEqual(String colName, ColumnType.Type type,
                                                     ScanPredicate.PredicateValue value) {
        switch (type) {
            case TINYINT:
                return FilterApi.gtEq(FilterApi.intColumn(colName), (int) value.getByte());
            case SMALLINT:
                return FilterApi.gtEq(FilterApi.intColumn(colName), (int) value.getShort());
            case INT:
                return FilterApi.gtEq(FilterApi.intColumn(colName), value.getInt());
            case BIGINT:
                return FilterApi.gtEq(FilterApi.longColumn(colName), value.getLong());
            case FLOAT:
                return FilterApi.gtEq(FilterApi.floatColumn(colName), value.getFloat());
            case DOUBLE:
                return FilterApi.gtEq(FilterApi.doubleColumn(colName), value.getDouble());
            case CHAR:
            case VARCHAR:
            case STRING:
                return FilterApi.gtEq(FilterApi.binaryColumn(colName), Binary.fromString(value.getString()));
            case BINARY:
                return FilterApi.gtEq(FilterApi.binaryColumn(colName), Binary.fromConstantByteArray(value.getBytes()));
            case ARRAY:
            case MAP:
            case STRUCT:
            default:
                throw new RuntimeException("Unsupported push_down_filter type value: " + type);
        }

    }

    private static FilterPredicate makeLess(String colName, ColumnType.Type type, ScanPredicate.PredicateValue value) {
        switch (type) {
            case TINYINT:
                return FilterApi.lt(FilterApi.intColumn(colName), (int) value.getByte());
            case SMALLINT:
                return FilterApi.lt(FilterApi.intColumn(colName), (int) value.getShort());
            case INT:
                return FilterApi.lt(FilterApi.intColumn(colName), value.getInt());
            case BIGINT:
                return FilterApi.lt(FilterApi.longColumn(colName), value.getLong());
            case FLOAT:
                return FilterApi.lt(FilterApi.floatColumn(colName), value.getFloat());
            case DOUBLE:
                return FilterApi.lt(FilterApi.doubleColumn(colName), value.getDouble());
            case CHAR:
            case VARCHAR:
            case STRING:
                return FilterApi.lt(FilterApi.binaryColumn(colName), Binary.fromString(value.getString()));
            case BINARY:
                return FilterApi.lt(FilterApi.binaryColumn(colName), Binary.fromConstantByteArray(value.getBytes()));
            case ARRAY:
            case MAP:
            case STRUCT:
            default:
                throw new RuntimeException("Unsupported push_down_filter type value: " + type);
        }

    }

    private static FilterPredicate makeLessOrEqual(String colName, ColumnType.Type type,
                                                   ScanPredicate.PredicateValue value) {
        switch (type) {
            case TINYINT:
                return FilterApi.ltEq(FilterApi.intColumn(colName), (int) value.getByte());
            case SMALLINT:
                return FilterApi.ltEq(FilterApi.intColumn(colName), (int) value.getShort());
            case INT:
                return FilterApi.ltEq(FilterApi.intColumn(colName), value.getInt());
            case BIGINT:
                return FilterApi.ltEq(FilterApi.longColumn(colName), value.getLong());
            case FLOAT:
                return FilterApi.ltEq(FilterApi.floatColumn(colName), value.getFloat());
            case DOUBLE:
                return FilterApi.ltEq(FilterApi.doubleColumn(colName), value.getDouble());
            case CHAR:
            case VARCHAR:
            case STRING:
                return FilterApi.ltEq(FilterApi.binaryColumn(colName), Binary.fromString(value.getString()));
            case BINARY:
                return FilterApi.ltEq(FilterApi.binaryColumn(colName), Binary.fromConstantByteArray(value.getBytes()));
            case ARRAY:
            case MAP:
            case STRUCT:
            default:
                throw new RuntimeException("Unsupported push_down_filter type value: " + type);
        }
    }
}
