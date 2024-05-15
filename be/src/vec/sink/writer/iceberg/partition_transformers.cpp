#include "vec/sink/writer/iceberg/partition_transformers.h"

#include <any>

#include "vec/core/types.h"
#include "vec/exec/format/table/iceberg/partition_spec.h"

namespace doris {
namespace vectorized {

std::unique_ptr<PartitionColumnTransform> PartitionColumnTransforms::create(
        const doris::iceberg::PartitionField& field) {
    auto& transform = field.transform();

    if (transform == "identity") {
        return std::make_unique<IdentityPartitionColumnTransform>();
    } else {
        throw doris::Exception(doris::ErrorCode::INTERNAL_ERROR,
                               "Unsupported partition transform: {}.", transform);
    }
}

std::string PartitionColumnTransform::to_human_string(const TypeDescriptor& type,
                                                      const std::any& value) const {
    if (value.has_value()) {
        switch (type.type) {
        case TYPE_BOOLEAN: {
            return std::to_string(std::any_cast<bool>(value));
        }
        case TYPE_TINYINT: {
            return std::to_string(std::any_cast<Int8>(value));
        }
        case TYPE_SMALLINT: {
            return std::to_string(std::any_cast<Int16>(value));
        }
        case TYPE_INT: {
            return std::to_string(std::any_cast<Int32>(value));
        }
        case TYPE_BIGINT: {
            return std::to_string(std::any_cast<Int64>(value));
        }
        case TYPE_FLOAT: {
            return std::to_string(std::any_cast<Float32>(value));
        }
        case TYPE_DOUBLE: {
            return std::to_string(std::any_cast<Float64>(value));
        }
        case TYPE_VARCHAR:
        case TYPE_CHAR:
        case TYPE_STRING: {
            return std::any_cast<std::string>(value);
        }
        case TYPE_DATE: {
            char buf[64];
            char* pos = std::any_cast<VecDateTimeValue>(value).to_string(buf);
            return std::string(buf, pos - buf - 1);
        }
        case TYPE_DATETIME: {
            char buf[64];
            char* pos = std::any_cast<VecDateTimeValue>(value).to_string(buf);
            return std::string(buf, pos - buf - 1);
        }
        case TYPE_DATEV2: {
            char buf[64];
            char* pos = std::any_cast<DateV2Value<DateV2ValueType>>(value).to_string(buf);
            return std::string(buf, pos - buf - 1);
        }
        case TYPE_DATETIMEV2: {
            char buf[64];
            char* pos = std::any_cast<DateV2Value<DateTimeV2ValueType>>(value).to_string(
                    buf, type.scale);
            return std::string(buf, pos - buf - 1);
        }
        case TYPE_DECIMALV2: {
            return std::any_cast<Decimal128V2>(value).to_string(type.scale);
        }
        case TYPE_DECIMAL32: {
            return std::any_cast<Decimal32>(value).to_string(type.scale);
        }
        case TYPE_DECIMAL64: {
            return std::any_cast<Decimal64>(value).to_string(type.scale);
        }
        case TYPE_DECIMAL128I: {
            return std::any_cast<Decimal128V3>(value).to_string(type.scale);
        }
        case TYPE_DECIMAL256: {
            return std::any_cast<Decimal256>(value).to_string(type.scale);
        }
        default: {
            throw doris::Exception(doris::ErrorCode::INTERNAL_ERROR,
                                   "Unsupported type for partition {}", type.debug_string());
        }
        }
    }
    return "null";
}

} // namespace vectorized
} // namespace doris