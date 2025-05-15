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

package org.apache.doris.nereids.trees.plans.commands.info;

import org.apache.doris.catalog.EncodingInfo;
import org.apache.doris.catalog.EncodingTree;
import org.apache.doris.nereids.types.AggStateType;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.CharType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DateTimeType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.DateType;
import org.apache.doris.nereids.types.DateV2Type;
import org.apache.doris.nereids.types.DecimalV2Type;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.FloatType;
import org.apache.doris.nereids.types.HllType;
import org.apache.doris.nereids.types.IPv4Type;
import org.apache.doris.nereids.types.IPv6Type;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.JsonType;
import org.apache.doris.nereids.types.LargeIntType;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.QuantileStateType;
import org.apache.doris.nereids.types.SmallIntType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.StructField;
import org.apache.doris.nereids.types.StructType;
import org.apache.doris.nereids.types.TinyIntType;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.nereids.types.VariantType;
import org.apache.doris.nereids.types.coercion.ComplexDataType;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import doris.segment_v2.SegmentV2;
import doris.segment_v2.SegmentV2.EncodingTypePB;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Used to store the type and encoding of a column.
 */
public class TypeAndEncoding {
    private static final Map<String, EncodingTypePB> encodingMap = new HashMap<>();
    private static final Map<Class<? extends DataType>, Set<EncodingTypePB>> supportedEncodingMap = new HashMap<>();

    static {
        // same as BE cod: encoding_info.cpp
        Set<EncodingTypePB> tinyIntEncoding = new HashSet<>();
        tinyIntEncoding.add(EncodingTypePB.BIT_SHUFFLE);
        tinyIntEncoding.add(EncodingTypePB.FOR_ENCODING);
        tinyIntEncoding.add(EncodingTypePB.PLAIN_ENCODING);
        supportedEncodingMap.put(TinyIntType.class, tinyIntEncoding);

        Set<EncodingTypePB> smallIntEncoding = new HashSet<>();
        smallIntEncoding.add(EncodingTypePB.BIT_SHUFFLE);
        smallIntEncoding.add(EncodingTypePB.FOR_ENCODING);
        smallIntEncoding.add(EncodingTypePB.PLAIN_ENCODING);
        supportedEncodingMap.put(SmallIntType.class, smallIntEncoding);

        Set<EncodingTypePB> intEncoding = new HashSet<>();
        intEncoding.add(EncodingTypePB.BIT_SHUFFLE);
        intEncoding.add(EncodingTypePB.FOR_ENCODING);
        intEncoding.add(EncodingTypePB.PLAIN_ENCODING);
        supportedEncodingMap.put(IntegerType.class, intEncoding);

        Set<EncodingTypePB> bigIntEncoding = new HashSet<>();
        bigIntEncoding.add(EncodingTypePB.BIT_SHUFFLE);
        bigIntEncoding.add(EncodingTypePB.FOR_ENCODING);
        bigIntEncoding.add(EncodingTypePB.PLAIN_ENCODING);
        supportedEncodingMap.put(BigIntType.class, bigIntEncoding);

        Set<EncodingTypePB> largeIntEncoding = new HashSet<>();
        largeIntEncoding.add(EncodingTypePB.BIT_SHUFFLE);
        largeIntEncoding.add(EncodingTypePB.PLAIN_ENCODING);
        largeIntEncoding.add(EncodingTypePB.FOR_ENCODING);
        supportedEncodingMap.put(LargeIntType.class, largeIntEncoding);

        Set<EncodingTypePB> floatEncoding = new HashSet<>();
        floatEncoding.add(EncodingTypePB.BIT_SHUFFLE);
        floatEncoding.add(EncodingTypePB.PLAIN_ENCODING);
        supportedEncodingMap.put(FloatType.class, floatEncoding);

        Set<EncodingTypePB> doubleEncoding = new HashSet<>();
        doubleEncoding.add(EncodingTypePB.BIT_SHUFFLE);
        doubleEncoding.add(EncodingTypePB.PLAIN_ENCODING);
        supportedEncodingMap.put(DoubleType.class, doubleEncoding);

        Set<EncodingTypePB> charLikeEncoding = new HashSet<>();
        charLikeEncoding.add(EncodingTypePB.DICT_ENCODING);
        charLikeEncoding.add(EncodingTypePB.PLAIN_ENCODING);
        charLikeEncoding.add(EncodingTypePB.PREFIX_ENCODING);
        supportedEncodingMap.put(CharType.class, charLikeEncoding);
        supportedEncodingMap.put(VarcharType.class, charLikeEncoding);
        supportedEncodingMap.put(StringType.class, charLikeEncoding);
        supportedEncodingMap.put(JsonType.class, charLikeEncoding);
        supportedEncodingMap.put(VariantType.class, charLikeEncoding);

        Set<EncodingTypePB> boolEncoding = new HashSet<>();
        boolEncoding.add(EncodingTypePB.RLE);
        boolEncoding.add(EncodingTypePB.BIT_SHUFFLE);
        boolEncoding.add(EncodingTypePB.PLAIN_ENCODING);
        supportedEncodingMap.put(BooleanType.class, boolEncoding);

        Set<EncodingTypePB> dateLikeEncoding = new HashSet<>();
        dateLikeEncoding.add(EncodingTypePB.BIT_SHUFFLE);
        dateLikeEncoding.add(EncodingTypePB.PLAIN_ENCODING);
        dateLikeEncoding.add(EncodingTypePB.FOR_ENCODING);
        supportedEncodingMap.put(DateType.class, dateLikeEncoding);
        supportedEncodingMap.put(DateV2Type.class, dateLikeEncoding);
        supportedEncodingMap.put(DateTimeType.class, dateLikeEncoding);
        supportedEncodingMap.put(DateTimeV2Type.class, dateLikeEncoding);

        Set<EncodingTypePB> decimalLikeEncoding = new HashSet<>();
        decimalLikeEncoding.add(EncodingTypePB.BIT_SHUFFLE);
        decimalLikeEncoding.add(EncodingTypePB.PLAIN_ENCODING);
        supportedEncodingMap.put(DecimalV2Type.class, decimalLikeEncoding);
        supportedEncodingMap.put(DecimalV3Type.class, decimalLikeEncoding);

        Set<EncodingTypePB> ipLikeEncoding = new HashSet<>();
        ipLikeEncoding.add(EncodingTypePB.BIT_SHUFFLE);
        ipLikeEncoding.add(EncodingTypePB.PLAIN_ENCODING);
        supportedEncodingMap.put(IPv4Type.class, ipLikeEncoding);
        supportedEncodingMap.put(IPv6Type.class, ipLikeEncoding);

        supportedEncodingMap.put(HllType.class, Sets.newHashSet(EncodingTypePB.PLAIN_ENCODING));

        supportedEncodingMap.put(QuantileStateType.class, Sets.newHashSet(EncodingTypePB.PLAIN_ENCODING));

        supportedEncodingMap.put(AggStateType.class, Sets.newHashSet(EncodingTypePB.PLAIN_ENCODING));
    }

    public final SegmentV2.EncodingTypePB encoding;

    public List<TypeAndEncoding> children;
    private DataType dataType;

    public TypeAndEncoding(DataType dataType, SegmentV2.EncodingTypePB encoding, List<TypeAndEncoding> children) {
        this.dataType = dataType;
        this.encoding = encoding;
        this.children = children;
    }

    public List<TypeAndEncoding> getChildren() {
        return children;
    }

    public void setChildren(List<TypeAndEncoding> children) {
        this.children = children;
    }

    public DataType getDataType() {
        return dataType;
    }

    public void setDataType(DataType dataType) {
        this.dataType = dataType;
    }

    /**
     * DataType conversion. For example DecimalV2Type to DecimalV3Type.
     */
    public TypeAndEncoding conversion() {
        if (dataType instanceof ComplexDataType) {
            List<TypeAndEncoding> newChildren = new ArrayList<>();
            for (TypeAndEncoding child : children) {
                newChildren.add(child.conversion());
            }
            if (dataType.isArrayType()) {
                DataType newItemType = newChildren.get(0).getDataType();
                return new TypeAndEncoding(ArrayType.of(newItemType, dataType.isNullType()), encoding, newChildren);
            }
            if (dataType.isStructType()) {
                List<StructField> newFields = new ArrayList<>();
                StructType oldDataType = (StructType) dataType;
                for (int i = 0; i < oldDataType.getFields().size(); i++) {
                    StructField oldChildField = oldDataType.getFields().get(i);
                    DataType newChildType = newChildren.get(i).getDataType();
                    newFields.add(oldChildField.withDataType(newChildType));
                }
                return new TypeAndEncoding(new StructType(newFields), encoding, newChildren);
            }
            if (dataType.isMapType()) {
                return new TypeAndEncoding(
                        MapType.of(newChildren.get(0).getDataType(), newChildren.get(1).getDataType()), encoding,
                        newChildren);
            }
            throw new IllegalArgumentException("Unsupported data type: " + dataType.toSql());
        }
        if (children != null && !children.isEmpty()) {
            throw new IllegalArgumentException(dataType.toSql() + " has children encoding");
        }
        return new TypeAndEncoding(dataType.conversion(), encoding, children);
    }

    /**
     * Get and check encoding.
     */
    public static TypeAndEncoding wrap(DataType dataType, String encoding, List<TypeAndEncoding> children) {
        if (Strings.isNullOrEmpty(encoding) || encoding.equalsIgnoreCase("DEFAULT")) {
            return new TypeAndEncoding(dataType, EncodingTypePB.DEFAULT_ENCODING, children);
        }
        if (dataType instanceof ComplexDataType) {
            return new TypeAndEncoding(dataType, EncodingTypePB.DEFAULT_ENCODING, children);
        }
        Integer encodingNumber = EncodingInfo.getEncodingNumber(encoding);
        if (encodingNumber == null) {
            throw new IllegalArgumentException("Unsupported encoding: " + encoding);
        } else if (supportedEncodingMap.containsKey(dataType.getClass())) {
            return new TypeAndEncoding(dataType, EncodingTypePB.forNumber(encodingNumber), children);
        } else {
            throw new IllegalArgumentException("Unsupported encoding: " + encoding + ", type: " + dataType.toSql());
        }
    }

    /**
     * wrap a data type with default encoding.
     */
    public static TypeAndEncoding forDefaultEncoding(DataType dataType) {
        if (dataType instanceof ComplexDataType) {
            List<TypeAndEncoding> children = Lists.newArrayList();
            if (dataType.isArrayType()) {
                ArrayType arrayType = (ArrayType) dataType;
                children.add(forDefaultEncoding(arrayType.getItemType()));
            } else if (dataType.isMapType()) {
                MapType mapType = (MapType) dataType;
                children.add(forDefaultEncoding(mapType.getKeyType()));
                children.add(forDefaultEncoding(mapType.getValueType()));
            } else if (dataType.isStructType()) {
                StructType structType = (StructType) dataType;
                structType.getFields().stream().map(StructField::getDataType)
                        .forEach(dt -> children.add(forDefaultEncoding(dt)));
            } else {
                throw new IllegalArgumentException("Unsupported complex data type: " + dataType.toSql());
            }
            return TypeAndEncoding.wrap(dataType, null, children);
        }
        return TypeAndEncoding.wrap(dataType, null, null);
    }

    public EncodingTree toEncodingTree() {
        List<EncodingTree> childrenTree = children == null ? null :
                children.stream().map(TypeAndEncoding::toEncodingTree).collect(Collectors.toList());
        return new EncodingTree(encoding.getNumber(), childrenTree);
    }
}
