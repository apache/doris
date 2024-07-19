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

#include "vec/sink/writer/iceberg/partition_transformers.h"

#include <any>

#include "vec/core/types.h"
#include "vec/exec/format/table/iceberg/partition_spec.h"

namespace doris {
namespace vectorized {

const std::chrono::sys_days PartitionColumnTransformUtils::EPOCH = std::chrono::sys_days(
        std::chrono::year {1970} / std::chrono::January / std::chrono::day {1});

std::unique_ptr<PartitionColumnTransform> PartitionColumnTransforms::create(
        const doris::iceberg::PartitionField& field, const TypeDescriptor& source_type) {
    auto& transform = field.transform();
    static const std::regex has_width(R"((\w+)\[(\d+)\])");
    std::smatch width_match;

    if (std::regex_match(transform, width_match, has_width)) {
        std::string name = width_match[1];
        int parsed_width = std::stoi(width_match[2]);

        if (name == "truncate") {
            switch (source_type.type) {
            case TYPE_INT: {
                return std::make_unique<IntegerTruncatePartitionColumnTransform>(source_type,
                                                                                 parsed_width);
            }
            case TYPE_BIGINT: {
                return std::make_unique<BigintTruncatePartitionColumnTransform>(source_type,
                                                                                parsed_width);
            }
            case TYPE_VARCHAR:
            case TYPE_CHAR:
            case TYPE_STRING: {
                return std::make_unique<StringTruncatePartitionColumnTransform>(source_type,
                                                                                parsed_width);
            }
            case TYPE_DECIMALV2: {
                return std::make_unique<DecimalTruncatePartitionColumnTransform<Decimal128V2>>(
                        source_type, parsed_width);
            }
            case TYPE_DECIMAL32: {
                return std::make_unique<DecimalTruncatePartitionColumnTransform<Decimal32>>(
                        source_type, parsed_width);
            }
            case TYPE_DECIMAL64: {
                return std::make_unique<DecimalTruncatePartitionColumnTransform<Decimal64>>(
                        source_type, parsed_width);
            }
            case TYPE_DECIMAL128I: {
                return std::make_unique<DecimalTruncatePartitionColumnTransform<Decimal128V3>>(
                        source_type, parsed_width);
            }
            case TYPE_DECIMAL256: {
                return std::make_unique<DecimalTruncatePartitionColumnTransform<Decimal256>>(
                        source_type, parsed_width);
            }
            default: {
                throw doris::Exception(doris::ErrorCode::INTERNAL_ERROR,
                                       "Unsupported type {} for partition column transform {}",
                                       source_type.debug_string(), transform);
            }
            }
        } else if (name == "bucket") {
            switch (source_type.type) {
            case TYPE_INT: {
                return std::make_unique<IntBucketPartitionColumnTransform>(source_type,
                                                                           parsed_width);
            }
            case TYPE_BIGINT: {
                return std::make_unique<BigintBucketPartitionColumnTransform>(source_type,
                                                                              parsed_width);
            }
            case TYPE_VARCHAR:
            case TYPE_CHAR:
            case TYPE_STRING: {
                return std::make_unique<StringBucketPartitionColumnTransform>(source_type,
                                                                              parsed_width);
            }
            case TYPE_DATEV2: {
                return std::make_unique<DateBucketPartitionColumnTransform>(source_type,
                                                                            parsed_width);
            }
            case TYPE_DATETIMEV2: {
                return std::make_unique<TimestampBucketPartitionColumnTransform>(source_type,
                                                                                 parsed_width);
            }
            case TYPE_DECIMALV2: {
                return std::make_unique<DecimalBucketPartitionColumnTransform<Decimal128V2>>(
                        source_type, parsed_width);
            }
            case TYPE_DECIMAL32: {
                return std::make_unique<DecimalBucketPartitionColumnTransform<Decimal32>>(
                        source_type, parsed_width);
            }
            case TYPE_DECIMAL64: {
                return std::make_unique<DecimalBucketPartitionColumnTransform<Decimal64>>(
                        source_type, parsed_width);
            }
            case TYPE_DECIMAL128I: {
                return std::make_unique<DecimalBucketPartitionColumnTransform<Decimal128V3>>(
                        source_type, parsed_width);
            }
            case TYPE_DECIMAL256: {
                return std::make_unique<DecimalBucketPartitionColumnTransform<Decimal256>>(
                        source_type, parsed_width);
            }
            default: {
                throw doris::Exception(doris::ErrorCode::INTERNAL_ERROR,
                                       "Unsupported type {} for partition column transform {}",
                                       source_type.debug_string(), transform);
            }
            }
        }
    }

    if (transform == "identity") {
        return std::make_unique<IdentityPartitionColumnTransform>(source_type);
    } else if (transform == "year") {
        switch (source_type.type) {
        case TYPE_DATEV2: {
            return std::make_unique<DateYearPartitionColumnTransform>(source_type);
        }
        case TYPE_DATETIMEV2: {
            return std::make_unique<TimestampYearPartitionColumnTransform>(source_type);
        }
        default: {
            throw doris::Exception(doris::ErrorCode::INTERNAL_ERROR,
                                   "Unsupported type {} for partition column transform {}",
                                   source_type.debug_string(), transform);
        }
        }
    } else if (transform == "month") {
        switch (source_type.type) {
        case TYPE_DATEV2: {
            return std::make_unique<DateMonthPartitionColumnTransform>(source_type);
        }
        case TYPE_DATETIMEV2: {
            return std::make_unique<TimestampMonthPartitionColumnTransform>(source_type);
        }
        default: {
            throw doris::Exception(doris::ErrorCode::INTERNAL_ERROR,
                                   "Unsupported type {} for partition column transform {}",
                                   source_type.debug_string(), transform);
        }
        }
    } else if (transform == "day") {
        switch (source_type.type) {
        case TYPE_DATEV2: {
            return std::make_unique<DateDayPartitionColumnTransform>(source_type);
        }
        case TYPE_DATETIMEV2: {
            return std::make_unique<TimestampDayPartitionColumnTransform>(source_type);
        }
        default: {
            throw doris::Exception(doris::ErrorCode::INTERNAL_ERROR,
                                   "Unsupported type {} for partition column transform {}",
                                   source_type.debug_string(), transform);
        }
        }
    } else if (transform == "hour") {
        switch (source_type.type) {
        case TYPE_DATETIMEV2: {
            return std::make_unique<TimestampHourPartitionColumnTransform>(source_type);
        }
        default: {
            throw doris::Exception(doris::ErrorCode::INTERNAL_ERROR,
                                   "Unsupported type {} for partition column transform {}",
                                   source_type.debug_string(), transform);
        }
        }
    } else if (transform == "void") {
        return std::make_unique<VoidPartitionColumnTransform>(source_type);
    } else {
        throw doris::Exception(doris::ErrorCode::INTERNAL_ERROR,
                               "Unsupported type {} for partition column transform {}",
                               source_type.debug_string(), transform);
    }
}

std::string PartitionColumnTransform::name() const {
    return "default";
}

std::string PartitionColumnTransform::to_human_string(const TypeDescriptor& type,
                                                      const std::any& value) const {
    return get_partition_value(type, value);
}

std::string PartitionColumnTransform::get_partition_value(const TypeDescriptor& type,
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
                                   "Unsupported type {} for partition", type.debug_string());
        }
        }
    }
    return "null";
}

} // namespace vectorized
} // namespace doris
