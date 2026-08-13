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

#include <fmt/format.h>

#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <utility>

#include "core/block/block.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "exec/sink/writer/iceberg/partition_transformers.h"
#include "exprs/function/function.h"
#include "exprs/function/simple_function_factory.h"
#include "format/table/iceberg/partition_spec.h"

namespace doris {
namespace {

enum class IcebergRoutingTransform { YEAR, MONTH, DAY, HOUR, BUCKET, TRUNCATE };

template <IcebergRoutingTransform transform>
struct IcebergRoutingTransformTraits;

#define DEFINE_ICEBERG_ROUTING_TRANSFORM(ENUM_VALUE, FUNCTION_NAME, TRANSFORM_NAME, HAS_PARAMETER) \
    template <>                                                                                    \
    struct IcebergRoutingTransformTraits<IcebergRoutingTransform::ENUM_VALUE> {                    \
        static constexpr auto function_name = FUNCTION_NAME;                                       \
        static constexpr auto transform_name = TRANSFORM_NAME;                                     \
        static constexpr bool has_parameter = HAS_PARAMETER;                                       \
    }

DEFINE_ICEBERG_ROUTING_TRANSFORM(YEAR, "__iceberg_transform_year", "year", false);
DEFINE_ICEBERG_ROUTING_TRANSFORM(MONTH, "__iceberg_transform_month", "month", false);
DEFINE_ICEBERG_ROUTING_TRANSFORM(DAY, "__iceberg_transform_day", "day", false);
DEFINE_ICEBERG_ROUTING_TRANSFORM(HOUR, "__iceberg_transform_hour", "hour", false);
DEFINE_ICEBERG_ROUTING_TRANSFORM(BUCKET, "__iceberg_transform_bucket", "bucket", true);
DEFINE_ICEBERG_ROUTING_TRANSFORM(TRUNCATE, "__iceberg_transform_truncate", "truncate", true);

#undef DEFINE_ICEBERG_ROUTING_TRANSFORM

template <IcebergRoutingTransform transform>
class FunctionIcebergPartitionTransform final : public IFunction {
    using Traits = IcebergRoutingTransformTraits<transform>;

public:
    static constexpr auto name = Traits::function_name;

    static FunctionPtr create() {
        return std::make_shared<FunctionIcebergPartitionTransform<transform>>();
    }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return Traits::has_parameter ? 2 : 1; }

    ColumnNumbers get_arguments_that_are_always_constant() const override {
        if constexpr (Traits::has_parameter) {
            return {1};
        }
        return {};
    }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        if constexpr (transform == IcebergRoutingTransform::TRUNCATE) {
            return arguments[0];
        }
        return std::make_shared<DataTypeInt32>();
    }

    Status execute_impl(FunctionContext*, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t) const override {
        std::string transform_spec = Traits::transform_name;
        if constexpr (Traits::has_parameter) {
            int64_t parameter = block.get_by_position(arguments[1]).column->get_int(0);
            if (parameter <= 0 || parameter > std::numeric_limits<int>::max()) {
                return Status::InvalidArgument("Invalid Iceberg {} parameter {}",
                                               Traits::transform_name, parameter);
            }
            transform_spec = fmt::format("{}[{}]", Traits::transform_name, parameter);
        }

        DataTypePtr source_type = remove_nullable(block.get_by_position(arguments[0]).type);
        iceberg::PartitionField field(0, 0, "__doris_write_route", transform_spec);
        std::unique_ptr<PartitionColumnTransform> transformer =
                PartitionColumnTransforms::create(field, source_type);
        ColumnWithTypeAndName transformed = transformer->apply(block, arguments[0]);
        block.replace_by_position(result, std::move(transformed.column));
        return Status::OK();
    }
};

} // namespace

void register_function_iceberg_partition_transform(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionIcebergPartitionTransform<IcebergRoutingTransform::YEAR>>();
    factory.register_function<FunctionIcebergPartitionTransform<IcebergRoutingTransform::MONTH>>();
    factory.register_function<FunctionIcebergPartitionTransform<IcebergRoutingTransform::DAY>>();
    factory.register_function<FunctionIcebergPartitionTransform<IcebergRoutingTransform::HOUR>>();
    factory.register_function<FunctionIcebergPartitionTransform<IcebergRoutingTransform::BUCKET>>();
    factory.register_function<
            FunctionIcebergPartitionTransform<IcebergRoutingTransform::TRUNCATE>>();
}

} // namespace doris
