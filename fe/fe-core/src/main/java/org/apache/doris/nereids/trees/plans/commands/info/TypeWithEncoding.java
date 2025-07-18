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
public class TypeWithEncoding {
    private static final Map<String, EncodingTypePB> encodingMap = new HashMap<>();
    private static final Map<Class<? extends DataType>, Set<Integer>> supportedEncodingMap = new HashMap<>();

    // Attention: Some types whose CPP type is `int128_t` can not support `PLAIN_ENCODING`.
    // Read plan encoding int128_t in our cluster will cause be core. The stack is as follows:
    // ==============================================================================================
    // *** SIGSEGV unknown detail explain (@0x0) received by PID 20460 (TID 20899 OR 0x7f6ed2bf9700) from PID 0;
    // stack trace: ***
    //  0# doris::signal::(anonymous namespace)::FailureSignalHandler(int, siginfo_t*, void*)
    //  at /root/doris/be/src/common/signal_handler.h:420
    //  1# 0x00007F6FB619D400 in /lib64/libc.so.6
    //  2# doris::segment_v2::PlainPageDecoder<(doris::FieldType)39>::read_by_rowids(unsigned int const*, unsigned long,
    //  unsigned long*, doris::COW<doris::vectorized::IColumn>::mutable_ptr<doris::vectorized::IColumn>&)
    //  at /root/doris/be/src/olap/rowset/segment_v2/plain_page.h:168
    //  3# doris::segment_v2::FileColumnIterator::read_by_rowids(unsigned int const*, unsigned long,
    //  doris::COW<doris::vectorized::IColumn>::mutable_ptr<doris::vectorized::IColumn>&)
    //  at /root/doris/be/src/olap/rowset/segment_v2/column_reader.cpp:1373
    //  4# doris::segment_v2::SegmentIterator::_read_columns_by_index(unsigned int, unsigned int&, bool)
    //  at /root/doris/be/src/olap/rowset/segment_v2/segment_iterator.cpp:1772
    //  5# doris::segment_v2::SegmentIterator::_next_batch_internal(doris::vectorized::Block*)
    //  at /root/doris/be/src/olap/rowset/segment_v2/segment_iterator.cpp:2122
    //  6# doris::segment_v2::SegmentIterator::next_batch(doris::vectorized::Block*)
    //  at /root/doris/be/src/olap/rowset/segment_v2/segment_iterator.cpp:1953
    //  7# doris::segment_v2::LazyInitSegmentIterator::next_batch(doris::vectorized::Block*)
    //  at /root/doris/be/src/olap/rowset/segment_v2/lazy_init_segment_iterator.h:49
    //  8# doris::BetaRowsetReader::next_block(doris::vectorized::Block*)
    //  at /root/doris/be/src/olap/rowset/beta_rowset_reader.cpp:341
    //  9# doris::vectorized::VCollectIterator::Level0Iterator::refresh_current_row()
    //  at /root/doris/be/src/vec/olap/vcollect_iterator.cpp:510
    // 10# doris::vectorized::VCollectIterator::Level0Iterator::ensure_first_row_ref()
    // at /root/doris/be/src/vec/olap/vcollect_iterator.cpp:484
    // 11# doris::vectorized::VCollectIterator::Level1Iterator::ensure_first_row_ref()
    // in /opt/doris/be/lib/doris_be
    // 12# doris::vectorized::VCollectIterator::build_heap(std::vector<std::shared_ptr<doris::RowsetReader>,
    // std::allocator<std::shared_ptr<doris::RowsetReader> > >&)
    // at /root/doris/be/src/vec/olap/vcollect_iterator.cpp:186
    // 13# doris::vectorized::BlockReader::_init_collect_iter(doris::TabletReader::ReaderParams const&)
    // at /root/doris/be/src/vec/olap/block_reader.cpp:152
    // 14# doris::vectorized::BlockReader::init(doris::TabletReader::ReaderParams const&)
    // at /root/doris/be/src/vec/olap/block_reader.cpp:226
    // 15# doris::vectorized::OlapScanner::open(doris::RuntimeState*)
    // at /root/doris/be/src/vec/exec/scan/olap_scanner.cpp:238
    // 16# doris::vectorized::ScannerScheduler::_scanner_scan(std::shared_ptr<doris::vectorized::ScannerContext>,
    // std::shared_ptr<doris::vectorized::ScanTask>) at /root/doris/be/src/vec/exec/scan/scanner_scheduler.cpp:178
    // ==============================================================================================
    // But, I don't know why. I have checked the core dump and don't find any NULL in current Stack frame.
    // So, we disable plain encoding for largeint, decimalv2/v3 and ipv6.
    static {
        // same as BE cod: encoding_info.cpp
        Set<Integer> tinyIntEncoding = new HashSet<>();
        tinyIntEncoding.add(EncodingTypePB.BIT_SHUFFLE.getNumber());
        // tinyIntEncoding.add(EncodingTypePB.FOR_ENCODING);
        tinyIntEncoding.add(EncodingTypePB.PLAIN_ENCODING.getNumber());
        supportedEncodingMap.put(TinyIntType.class, tinyIntEncoding);

        Set<Integer> smallIntEncoding = new HashSet<>();
        smallIntEncoding.add(EncodingTypePB.BIT_SHUFFLE.getNumber());
        // smallIntEncoding.add(EncodingTypePB.FOR_ENCODING);
        smallIntEncoding.add(EncodingTypePB.PLAIN_ENCODING.getNumber());
        supportedEncodingMap.put(SmallIntType.class, smallIntEncoding);

        Set<Integer> intEncoding = new HashSet<>();
        intEncoding.add(EncodingTypePB.BIT_SHUFFLE.getNumber());
        // intEncoding.add(EncodingTypePB.FOR_ENCODING);
        intEncoding.add(EncodingTypePB.PLAIN_ENCODING.getNumber());
        supportedEncodingMap.put(IntegerType.class, intEncoding);

        Set<Integer> bigIntEncoding = new HashSet<>();
        bigIntEncoding.add(EncodingTypePB.BIT_SHUFFLE.getNumber());
        // bigIntEncoding.add(EncodingTypePB.FOR_ENCODING);
        bigIntEncoding.add(EncodingTypePB.PLAIN_ENCODING.getNumber());
        supportedEncodingMap.put(BigIntType.class, bigIntEncoding);

        Set<Integer> largeIntEncoding = new HashSet<>();
        largeIntEncoding.add(EncodingTypePB.BIT_SHUFFLE.getNumber());

        // See Attention comment: largeIntEncoding.add(EncodingTypePB.PLAIN_ENCODING.getNumber());
        // largeIntEncoding.add(EncodingTypePB.FOR_ENCODING);
        supportedEncodingMap.put(LargeIntType.class, largeIntEncoding);

        Set<Integer> floatEncoding = new HashSet<>();
        floatEncoding.add(EncodingTypePB.BIT_SHUFFLE.getNumber());
        floatEncoding.add(EncodingTypePB.PLAIN_ENCODING.getNumber());
        supportedEncodingMap.put(FloatType.class, floatEncoding);

        Set<Integer> doubleEncoding = new HashSet<>();
        doubleEncoding.add(EncodingTypePB.BIT_SHUFFLE.getNumber());
        doubleEncoding.add(EncodingTypePB.PLAIN_ENCODING.getNumber());
        supportedEncodingMap.put(DoubleType.class, doubleEncoding);

        Set<Integer> charLikeEncoding = new HashSet<>();
        charLikeEncoding.add(EncodingTypePB.DICT_ENCODING.getNumber());
        charLikeEncoding.add(EncodingTypePB.PLAIN_ENCODING.getNumber());
        // charLikeEncoding.add(EncodingTypePB.PREFIX_ENCODING);
        supportedEncodingMap.put(CharType.class, charLikeEncoding);
        supportedEncodingMap.put(VarcharType.class, charLikeEncoding);
        supportedEncodingMap.put(StringType.class, charLikeEncoding);
        supportedEncodingMap.put(JsonType.class, charLikeEncoding);
        supportedEncodingMap.put(VariantType.class, charLikeEncoding);

        Set<Integer> boolEncoding = new HashSet<>();
        boolEncoding.add(EncodingTypePB.RLE.getNumber());
        boolEncoding.add(EncodingTypePB.BIT_SHUFFLE.getNumber());
        boolEncoding.add(EncodingTypePB.PLAIN_ENCODING.getNumber());
        supportedEncodingMap.put(BooleanType.class, boolEncoding);

        Set<Integer> dateLikeEncoding = new HashSet<>();
        dateLikeEncoding.add(EncodingTypePB.BIT_SHUFFLE.getNumber());
        dateLikeEncoding.add(EncodingTypePB.PLAIN_ENCODING.getNumber());
        // dateLikeEncoding.add(EncodingTypePB.FOR_ENCODING);
        supportedEncodingMap.put(DateType.class, dateLikeEncoding);
        supportedEncodingMap.put(DateV2Type.class, dateLikeEncoding);
        supportedEncodingMap.put(DateTimeType.class, dateLikeEncoding);
        supportedEncodingMap.put(DateTimeV2Type.class, dateLikeEncoding);

        Set<Integer> decimalLikeEncoding = new HashSet<>();
        decimalLikeEncoding.add(EncodingTypePB.BIT_SHUFFLE.getNumber());
        // See Attention comment: decimalLikeEncoding.add(EncodingTypePB.PLAIN_ENCODING.getNumber());
        supportedEncodingMap.put(DecimalV2Type.class, decimalLikeEncoding);
        supportedEncodingMap.put(DecimalV3Type.class, decimalLikeEncoding);

        Set<Integer> ipV4Encoding = new HashSet<>();
        ipV4Encoding.add(EncodingTypePB.BIT_SHUFFLE.getNumber());
        ipV4Encoding.add(EncodingTypePB.PLAIN_ENCODING.getNumber());
        supportedEncodingMap.put(IPv4Type.class, ipV4Encoding);

        Set<Integer> ipV6Encoding = new HashSet<>();
        ipV6Encoding.add(EncodingTypePB.BIT_SHUFFLE.getNumber());
        // See Attention comment: ipV6Encoding.add(EncodingTypePB.PLAIN_ENCODING.getNumber());
        supportedEncodingMap.put(IPv6Type.class, ipV6Encoding);

        supportedEncodingMap.put(HllType.class, Sets.newHashSet(EncodingTypePB.PLAIN_ENCODING.getNumber()));

        supportedEncodingMap.put(QuantileStateType.class, Sets.newHashSet(EncodingTypePB.PLAIN_ENCODING.getNumber()));

        supportedEncodingMap.put(AggStateType.class, Sets.newHashSet(EncodingTypePB.PLAIN_ENCODING.getNumber()));
    }

    public final SegmentV2.EncodingTypePB encoding;

    public List<TypeWithEncoding> children;
    private DataType dataType;

    public TypeWithEncoding(DataType dataType, SegmentV2.EncodingTypePB encoding, List<TypeWithEncoding> children) {
        this.dataType = dataType;
        this.encoding = encoding;
        this.children = children;
    }

    public List<TypeWithEncoding> getChildren() {
        return children;
    }

    public void setChildren(List<TypeWithEncoding> children) {
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
    public TypeWithEncoding conversion() {
        if (dataType instanceof ComplexDataType) {
            List<TypeWithEncoding> newChildren = new ArrayList<>();
            for (TypeWithEncoding child : children) {
                newChildren.add(child.conversion());
            }
            if (dataType.isArrayType()) {
                DataType newItemType = newChildren.get(0).getDataType();
                return new TypeWithEncoding(ArrayType.of(newItemType, dataType.isNullType()), encoding, newChildren);
            }
            if (dataType.isStructType()) {
                List<StructField> newFields = new ArrayList<>();
                StructType oldDataType = (StructType) dataType;
                for (int i = 0; i < oldDataType.getFields().size(); i++) {
                    StructField oldChildField = oldDataType.getFields().get(i);
                    DataType newChildType = newChildren.get(i).getDataType();
                    newFields.add(oldChildField.withDataType(newChildType));
                }
                return new TypeWithEncoding(new StructType(newFields), encoding, newChildren);
            }
            if (dataType.isMapType()) {
                return new TypeWithEncoding(
                        MapType.of(newChildren.get(0).getDataType(), newChildren.get(1).getDataType()), encoding,
                        newChildren);
            }
            throw new IllegalArgumentException("Unsupported data type: " + dataType.toSql());
        }
        if (children != null && !children.isEmpty()) {
            throw new IllegalArgumentException(dataType.toSql() + " has children encoding");
        }
        return new TypeWithEncoding(dataType.conversion(), encoding, children);
    }

    /**
     * Get and check encoding.
     */
    public static TypeWithEncoding wrap(DataType dataType, String encoding, List<TypeWithEncoding> children) {
        if (Strings.isNullOrEmpty(encoding) || encoding.equalsIgnoreCase("DEFAULT")) {
            return new TypeWithEncoding(dataType, EncodingTypePB.DEFAULT_ENCODING, children);
        }
        if (dataType instanceof ComplexDataType) {
            return new TypeWithEncoding(dataType, EncodingTypePB.DEFAULT_ENCODING, children);
        }
        Integer encodingNumber = EncodingInfo.getEncodingNumber(encoding);
        if (encodingNumber == null) {
            throw new IllegalArgumentException("Unsupported encoding: " + encoding);
        } else if (supportedEncodingMap.containsKey(dataType.getClass())
                && supportedEncodingMap.get(dataType.getClass()).contains(encodingNumber)) {
            return new TypeWithEncoding(dataType, EncodingTypePB.forNumber(encodingNumber), children);
        } else {
            throw new IllegalArgumentException("Unsupported encoding: " + encoding + ", type: " + dataType.toSql());
        }
    }

    /**
     * wrap a data type with default encoding.
     */
    public static TypeWithEncoding forDefaultEncoding(DataType dataType) {
        if (dataType instanceof ComplexDataType) {
            List<TypeWithEncoding> children = Lists.newArrayList();
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
            return TypeWithEncoding.wrap(dataType, null, children);
        }
        return TypeWithEncoding.wrap(dataType, null, null);
    }

    public EncodingTree toEncodingTree() {
        List<EncodingTree> childrenTree = children == null ? null :
                children.stream().map(TypeWithEncoding::toEncodingTree).collect(Collectors.toList());
        return new EncodingTree(encoding.getNumber(), childrenTree);
    }
}
