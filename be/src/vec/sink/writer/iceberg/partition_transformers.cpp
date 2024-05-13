#include "vec/sink/writer/iceberg/partition_transformers.h"

#include <string>

#include "common/exception.h"
#include "vec/exec/format/table/iceberg/partition_spec.h"
#include "vec/exec/format/table/iceberg/transforms.h"

namespace doris {
namespace vectorized {

PartitionColumnTransform::PartitionColumnTransform(
        doris::iceberg::Type& type, bool preserves_non_null, bool monotonic, bool temporal,
        const BlockTransformFunctionType& block_transform,
        const ValueTransformFunctionType& value_transform)
        : _type(type),
          _preserves_non_null(preserves_non_null),
          _monotonic(monotonic),
          _temporal(temporal),
          _block_transform(block_transform),
          _value_transform(value_transform) {}

PartitionColumnTransform PartitionColumnTransform::create(
        const doris::iceberg::PartitionField& field, doris::iceberg::Type& source_type) {
    std::string transform = field.transform().to_string();

    if (transform == "identity") {
        return PartitionColumnTransform(
                source_type, false, true, false, [](IColumn& column) -> IColumn& { return column; },
                [](IColumn& column, int position) -> std::optional<int64_t> {
                    return std::nullopt;
                });
    } else {
        throw doris::Exception(doris::ErrorCode::INTERNAL_ERROR,
                               "Unsupported partition transform: {}.", transform);
    }
}

} // namespace vectorized
} // namespace doris