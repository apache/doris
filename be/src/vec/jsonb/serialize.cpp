#include "vec/jsonb/serialize.h"

#include "olap/hll.h"
#include "olap/tablet_schema.h"
#include "runtime/jsonb_value.h"
#include "util/jsonb_stream.h"
#include "util/jsonb_writer.h"
#include "vec/common/arena.h"
#include "vec/core/types.h"

namespace doris::vectorized {

static inline bool is_column_null_at(int row, const IColumn* column, const doris::FieldType& type,
                                     const StringRef& data_ref) {
    if (type != OLAP_FIELD_TYPE_ARRAY) {
        return data_ref.data == nullptr;
    } else {
        Field array;
        column->get(row, array);
        return array.is_null();
    }
}

static bool is_jsonb_blob_type(FieldType type) {
    return type == OLAP_FIELD_TYPE_VARCHAR || type == OLAP_FIELD_TYPE_CHAR ||
           type == OLAP_FIELD_TYPE_STRING || type == OLAP_FIELD_TYPE_STRUCT ||
           type == OLAP_FIELD_TYPE_ARRAY || type == OLAP_FIELD_TYPE_MAP ||
           type == OLAP_FIELD_TYPE_HLL || type == OLAP_FIELD_TYPE_OBJECT ||
           type == OLAP_FIELD_TYPE_JSONB;
}

// jsonb -> column value
static void deserialize_column(PrimitiveType type, JsonbValue* slot_value, MutableColumnPtr& dst) {
    if (type == TYPE_ARRAY) {
        assert(slot_value->isBinary());
        auto blob = static_cast<JsonbBlobVal*>(slot_value);
        dst->deserialize_and_insert_from_arena(blob->getBlob());
    } else if (type == TYPE_OBJECT) {
        assert(slot_value->isBinary());
        // TODO
    } else if (type == TYPE_HLL) {
        assert(slot_value->isBinary());
        // TODO
    } else if (is_string_type(type)) {
        assert(slot_value->isBinary());
        auto blob = static_cast<JsonbBlobVal*>(slot_value);
        dst->insert_data(blob->getBlob(), blob->getBlobLen());
    } else {
        switch (type) {
        case TYPE_BOOLEAN: {
            assert(slot_value->isInt8());
            dst->insert(static_cast<JsonbInt8Val*>(slot_value)->val());
            break;
        }
        case TYPE_TINYINT: {
            assert(slot_value->isInt8());
            dst->insert(static_cast<JsonbInt8Val*>(slot_value)->val());
            break;
        }
        case TYPE_SMALLINT: {
            assert(slot_value->isInt16());
            dst->insert(static_cast<JsonbInt16Val*>(slot_value)->val());
            break;
        }
        case TYPE_INT: {
            assert(slot_value->isInt32());
            dst->insert(static_cast<JsonbInt32Val*>(slot_value)->val());
            break;
        }
        case TYPE_BIGINT: {
            assert(slot_value->isInt64());
            dst->insert(static_cast<JsonbInt64Val*>(slot_value)->val());
            break;
        }
        case TYPE_LARGEINT: {
            assert(slot_value->isInt128());
            dst->insert(static_cast<JsonbInt128Val*>(slot_value)->val());
            break;
        }
        case TYPE_FLOAT:
        case TYPE_DOUBLE: {
            assert(slot_value->isDouble());
            dst->insert(static_cast<JsonbDoubleVal*>(slot_value)->val());
            break;
        }
        case TYPE_DATE: {
            assert(slot_value->isInt32());
            int32_t val = static_cast<JsonbInt32Val*>(slot_value)->val();
            dst->insert_many_fix_len_data(reinterpret_cast<const char*>(&val), 1);
            break;
        }
        case TYPE_DATETIME: {
            assert(slot_value->isInt64());
            int64_t val = static_cast<JsonbInt64Val*>(slot_value)->val();
            dst->insert_many_fix_len_data(reinterpret_cast<const char*>(&val), 1);
            break;
        }
        case TYPE_DATEV2: {
            assert(slot_value->isInt32());
            dst->insert(static_cast<JsonbInt32Val*>(slot_value)->val());
            break;
        }
        case TYPE_DATETIMEV2: {
            assert(slot_value->isInt64());
            dst->insert(static_cast<JsonbInt64Val*>(slot_value)->val());
            break;
        }
        case TYPE_DECIMAL32: {
            assert(slot_value->isInt32());
            dst->insert(static_cast<JsonbInt32Val*>(slot_value)->val());
            break;
        }
        case TYPE_DECIMAL64: {
            assert(slot_value->isInt64());
            dst->insert(static_cast<JsonbInt64Val*>(slot_value)->val());
            break;
        }
        case TYPE_DECIMAL128I: {
            assert(slot_value->isInt128());
            dst->insert(static_cast<JsonbInt128Val*>(slot_value)->val());
            break;
        }
        default:
            LOG(FATAL) << "unknow type " << type;
            break;
        }
    }
}

// column value -> jsonb
static void serialize_column(Arena* mem_pool, const TabletColumn& tablet_column,
                             const IColumn* column, const StringRef& data_ref, int row,
                             JsonbWriterT<JsonbOutStream>& jsonb_writer) {
    jsonb_writer.writeKey(tablet_column.unique_id());
    if (is_column_null_at(row, column, tablet_column.type(), data_ref)) {
        jsonb_writer.writeNull();
        return;
    }
    if (tablet_column.is_array_type()) {
        const char* begin = nullptr;
        StringRef value = column->serialize_value_into_arena(row, *mem_pool, begin);
        jsonb_writer.writeStartBinary();
        jsonb_writer.writeBinary(value.data, value.size);
        jsonb_writer.writeEndBinary();
    } else if (tablet_column.type() == OLAP_FIELD_TYPE_OBJECT) {
        auto bitmap_value = (BitmapValue*)(data_ref.data);
        auto size = bitmap_value->getSizeInBytes();
        // serialize the content of string
        auto ptr = mem_pool->alloc(size);
        bitmap_value->write(reinterpret_cast<char*>(ptr));
        jsonb_writer.writeStartBinary();
        jsonb_writer.writeBinary(reinterpret_cast<const char*>(ptr), size);
        jsonb_writer.writeEndBinary();
    } else if (tablet_column.type() == OLAP_FIELD_TYPE_HLL) {
        auto hll_value = (HyperLogLog*)(data_ref.data);
        auto size = hll_value->max_serialized_size();
        auto ptr = reinterpret_cast<char*>(mem_pool->alloc(size));
        size_t actual_size = hll_value->serialize((uint8_t*)ptr);
        jsonb_writer.writeStartBinary();
        jsonb_writer.writeBinary(reinterpret_cast<const char*>(ptr), actual_size);
        jsonb_writer.writeEndBinary();
    } else if (is_jsonb_blob_type(tablet_column.type())) {
        jsonb_writer.writeStartBinary();
        jsonb_writer.writeBinary(reinterpret_cast<const char*>(data_ref.data), data_ref.size);
        jsonb_writer.writeEndBinary();
    } else {
        switch (tablet_column.type()) {
        case OLAP_FIELD_TYPE_BOOL: {
            int8_t val = *reinterpret_cast<const int8_t*>(data_ref.data);
            jsonb_writer.writeInt8(val);
            break;
        }
        case OLAP_FIELD_TYPE_TINYINT: {
            int8_t val = *reinterpret_cast<const int8_t*>(data_ref.data);
            jsonb_writer.writeInt8(val);
            break;
        }
        case OLAP_FIELD_TYPE_SMALLINT: {
            int16_t val = *reinterpret_cast<const int16_t*>(data_ref.data);
            jsonb_writer.writeInt16(val);
            break;
        }
        case OLAP_FIELD_TYPE_INT: {
            int32_t val = *reinterpret_cast<const int32_t*>(data_ref.data);
            jsonb_writer.writeInt32(val);
            break;
        }
        case OLAP_FIELD_TYPE_BIGINT: {
            int64_t val = *reinterpret_cast<const int64_t*>(data_ref.data);
            jsonb_writer.writeInt64(val);
            break;
        }
        case OLAP_FIELD_TYPE_LARGEINT: {
            __int128_t val = *reinterpret_cast<const __int128_t*>(data_ref.data);
            jsonb_writer.writeInt128(val);
            break;
        }
        case OLAP_FIELD_TYPE_FLOAT: {
            float val = *reinterpret_cast<const float*>(data_ref.data);
            jsonb_writer.writeDouble(val);
            break;
        }
        case OLAP_FIELD_TYPE_DOUBLE: {
            double val = *reinterpret_cast<const double*>(data_ref.data);
            jsonb_writer.writeDouble(val);
            break;
        }
        case OLAP_FIELD_TYPE_DATE: {
            const auto* datetime_cur = reinterpret_cast<const VecDateTimeValue*>(data_ref.data);
            jsonb_writer.writeInt32(datetime_cur->to_olap_date());
            break;
        }
        case OLAP_FIELD_TYPE_DATETIME: {
            const auto* datetime_cur = reinterpret_cast<const VecDateTimeValue*>(data_ref.data);
            jsonb_writer.writeInt64(datetime_cur->to_olap_datetime());
            break;
        }
        case OLAP_FIELD_TYPE_DATEV2: {
            uint32_t val = *reinterpret_cast<const uint32_t*>(data_ref.data);
            jsonb_writer.writeInt32(val);
            break;
        }
        case OLAP_FIELD_TYPE_DATETIMEV2: {
            uint64_t val = *reinterpret_cast<const uint64_t*>(data_ref.data);
            jsonb_writer.writeInt64(val);
            break;
        }
        case OLAP_FIELD_TYPE_DECIMAL32: {
            Decimal32::NativeType val =
                    *reinterpret_cast<const Decimal32::NativeType*>(data_ref.data);
            jsonb_writer.writeInt32(val);
            break;
        }
        case OLAP_FIELD_TYPE_DECIMAL64: {
            Decimal64::NativeType val =
                    *reinterpret_cast<const Decimal64::NativeType*>(data_ref.data);
            jsonb_writer.writeInt64(val);
            break;
        }
        case OLAP_FIELD_TYPE_DECIMAL128I: {
            Decimal128I::NativeType val =
                    *reinterpret_cast<const Decimal128I::NativeType*>(data_ref.data);
            jsonb_writer.writeInt128(val);
            break;
        }
        case OLAP_FIELD_TYPE_DECIMAL:
            LOG(FATAL) << "OLAP_FIELD_TYPE_DECIMAL not implemented use DecimalV3 instead";
            break;
        default:
            LOG(FATAL) << "unknow type " << tablet_column.type();
            break;
        }
    }
}

void JsonbSerializeUtil::block_to_jsonb(const TabletSchema& schema, const Block& block,
                                        ColumnString& dst, int num_cols) {
    auto num_rows = block.rows();
    Arena pool;
    assert(num_cols <= block.columns());
    for (int i = 0; i < num_rows; ++i) {
        JsonbWriterT<JsonbOutStream> jsonb_writer;
        jsonb_writer.writeStartObject();
        for (int j = 0; j < num_cols; ++j) {
            const auto& column = block.get_by_position(j).column;
            const auto& tablet_column = schema.columns()[j];
            const auto& data_ref =
                    !tablet_column.is_array_type() ? column->get_data_at(i) : StringRef();
            serialize_column(&pool, tablet_column, column.get(), data_ref, i, jsonb_writer);
        }
        jsonb_writer.writeEndObject();
        dst.insert_data(jsonb_writer.getOutput()->getBuffer(), jsonb_writer.getOutput()->getSize());
    }
}

void JsonbSerializeUtil::jsonb_to_block(const TupleDescriptor& desc,
                                        const ColumnString& jsonb_column, Block& dst) {
    for (int i = 0; i < jsonb_column.size(); ++i) {
        StringRef jsonb_data = jsonb_column.get_data_at(i);
        auto pdoc = JsonbDocument::createDocument(jsonb_data.data, jsonb_data.size);
        JsonbDocument& doc = *pdoc;
        for (int j = 0; j < desc.slots().size(); ++j) {
            SlotDescriptor* slot = desc.slots()[j];
            JsonbValue* slot_value = doc->find(slot->col_unique_id());
            MutableColumnPtr dst_column = dst.get_by_position(j).column->assume_mutable();
            if (!slot_value || slot_value->isNull()) {
                // null or not exist
                dst_column->insert_default();
                continue;
            }
            deserialize_column(slot->type().type, slot_value, dst_column);
        }
    }
}

} // namespace doris::vectorized