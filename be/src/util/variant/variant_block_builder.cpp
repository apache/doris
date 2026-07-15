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

#include "util/variant/variant_block_builder.h"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <limits>
#include <string>
#include <utility>

#include "common/exception.h"
#include "util/utf8_check.h"
#include "util/variant/variant_block_builder_internal.h"
#include "util/variant/variant_encoded_block.h"
#include "util/variant/variant_encoding.h"
#include "util/variant/variant_scalar_encoding.h"
#include "util/variant/variant_tracked_storage.h"

namespace doris {
namespace {

constexpr uint32_t INVALID_INDEX = std::numeric_limits<uint32_t>::max();
constexpr uint64_t FINISHED_ROW_GENERATION = std::numeric_limits<uint64_t>::max();
constexpr uint64_t ABORTED_ROW_GENERATION = FINISHED_ROW_GENERATION - 1;
constexpr size_t INLINE_SCALAR_CAPACITY = sizeof(uint32_t);
constexpr size_t SMALL_OBJECT_SORT_THRESHOLD = 16;

constexpr unsigned __int128 max_decimal38() {
    unsigned __int128 value = 1;
    for (uint8_t digit = 0; digit < 38; ++digit) {
        value *= 10;
    }
    return value - 1;
}

constexpr unsigned __int128 MAX_DECIMAL38 = max_decimal38();

template <typename Container, typename Less>
void sort_object_entries(Container& entries, size_t begin, size_t count, Less less) {
    if (count < 2) {
        return;
    }
    if (count <= SMALL_OBJECT_SORT_THRESHOLD) {
        for (size_t index = begin + 1; index < begin + count; ++index) {
            typename Container::value_type current = entries[index];
            size_t insertion = index;
            while (insertion > begin && less(current, entries[insertion - 1])) {
                entries[insertion] = entries[insertion - 1];
                --insertion;
            }
            entries[insertion] = current;
        }
        return;
    }
    std::sort(entries.begin() + begin, entries.begin() + begin + count, less);
}

unsigned __int128 magnitude(__int128 value) {
    const auto unsigned_value = static_cast<unsigned __int128>(value);
    return value < 0 ? ~unsigned_value + 1 : unsigned_value;
}

uint8_t minimum_unsigned_width(uint64_t value) {
    if (value <= std::numeric_limits<uint8_t>::max()) {
        return 1;
    }
    if (value <= std::numeric_limits<uint16_t>::max()) {
        return 2;
    }
    if (value <= 0xFFFFFFU) {
        return 3;
    }
    return 4;
}

void write_unsigned(char*& output, uint64_t value, uint8_t width) noexcept {
    for (uint8_t byte = 0; byte < width; ++byte) {
        *output++ = static_cast<char>(value >> (byte * 8));
    }
}

void validate_string_ref(StringRef value, const char* description) {
    if (value.data == nullptr && value.size != 0) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "Variant {} has a null data pointer",
                        description);
    }
}

void validate_import_root(VariantValueRef value) {
    if (value.metadata.data == nullptr && value.metadata.size != 0) {
        throw Exception(ErrorCode::CORRUPTION,
                        "Variant imported metadata has a null data pointer for {} bytes",
                        value.metadata.size);
    }
    if (value.data == nullptr && value.size != 0) {
        throw Exception(ErrorCode::CORRUPTION,
                        "Variant imported value has a null data pointer for {} bytes", value.size);
    }
    value.metadata.validate();
}

void require_import_depth(uint32_t depth) {
    if (depth > VARIANT_MAX_NESTING_DEPTH) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant import exceeds maximum nesting depth {}",
                        VARIANT_MAX_NESTING_DEPTH);
    }
}

void require_exact_import_value(VariantValueRef value) {
    const size_t encoded_size = value.value_size();
    if (encoded_size != value.size) {
        throw Exception(ErrorCode::CORRUPTION,
                        "Variant imported value has {} trailing bytes after its {} byte root",
                        value.size - encoded_size, encoded_size);
    }
}

void require_import_utf8(StringRef value, const char* description) {
    if (value.size != 0 && !validate_utf8(value.data, value.size)) {
        throw Exception(ErrorCode::CORRUPTION, "Variant imported {} is not valid UTF-8",
                        description);
    }
}

} // namespace

class VariantCollectionCore {
public:
    enum class NodeKind : uint8_t { SCALAR, INLINE_SCALAR, OBJECT, ARRAY };
    enum class State : uint8_t { COLLECTING, COLLECTED, FINISHED, ABORTED };

    struct Node {
        Node() noexcept : kind(NodeKind::SCALAR), payload_index(0), payload_size(0) {}
        Node(NodeKind kind_, uint32_t payload_index_, uint32_t payload_size_) noexcept
                : kind(kind_), payload_index(payload_index_), payload_size(payload_size_) {}
        Node(std::array<char, INLINE_SCALAR_CAPACITY> inline_bytes_,
             uint32_t payload_size_) noexcept
                : kind(NodeKind::INLINE_SCALAR),
                  inline_bytes(inline_bytes_),
                  payload_size(payload_size_) {}

        NodeKind kind;
        union {
            uint32_t payload_index;
            std::array<char, INLINE_SCALAR_CAPACITY> inline_bytes;
        };
        uint32_t payload_size;
    };
    static_assert(sizeof(Node) == 12);

    struct Child {
        uint32_t node_index;
        uint32_t temporary_field_id;
        uint32_t next;
    };

    struct Container {
        explicit Container(NodeKind kind_) : kind(kind_) {}

        NodeKind kind;
        uint32_t first_child = INVALID_INDEX;
        uint32_t last_child = INVALID_INDEX;
        uint32_t child_count = 0;
        uint32_t pending_field_id = 0;
        uint32_t previous_or_source_token = INVALID_INDEX;
        uint32_t previous_child_cursor = INVALID_INDEX;
        bool has_pending_field = false;
    };

    struct Checkpoint {
        size_t scalar_bytes;
        size_t nodes;
        size_t containers;
        size_t children;
        size_t key_references;
    };

    struct CapacitySnapshot {
        size_t scalar_bytes;
        size_t nodes;
        size_t containers;
        size_t children;
        size_t scope_stack;
        size_t object_id_scratch;
        size_t key_references;
        size_t container_plans;
        size_t planned_object_children;
    };

    struct FinalChild {
        uint32_t node_index;
        uint32_t final_field_id;
    };

    struct ContainerPlan {
        size_t encoded_size = 0;
        size_t object_children_begin = 0;
        uint32_t values_size = 0;
        uint8_t count_width = 0;
        uint8_t offset_width = 0;
        uint8_t id_width = 0;
    };

    explicit VariantCollectionCore(VariantMetadataBuilder& metadata_) : metadata(metadata_) {}

    void reserve(size_t scalar_byte_count, size_t node_count, size_t container_count,
                 size_t child_count) {
        scalar_bytes.reserve(scalar_byte_count);
        nodes.reserve(node_count);
        containers.reserve(container_count);
        children.reserve(child_count);
        scope_stack.reserve(container_count);
        object_id_scratch.reserve(child_count);
        key_references.reserve(child_count);
        container_plans.reserve(container_count);
        planned_object_children.reserve(child_count);
    }

    CapacitySnapshot capacity_snapshot() const noexcept {
        return {.scalar_bytes = scalar_bytes.capacity(),
                .nodes = nodes.capacity(),
                .containers = containers.capacity(),
                .children = children.capacity(),
                .scope_stack = scope_stack.capacity(),
                .object_id_scratch = object_id_scratch.capacity(),
                .key_references = key_references.capacity(),
                .container_plans = container_plans.capacity(),
                .planned_object_children = planned_object_children.capacity()};
    }

    Checkpoint checkpoint() const noexcept {
        return {.scalar_bytes = scalar_bytes.size(),
                .nodes = nodes.size(),
                .containers = containers.size(),
                .children = children.size(),
                .key_references = key_references.size()};
    }

    void begin_collection(DorisVector<uint32_t>* previous_tokens = nullptr,
                          DorisVector<uint32_t>* pending_tokens = nullptr) noexcept {
        DCHECK(scope_stack.empty());
        DCHECK_EQ(active_container, INVALID_INDEX);
        DCHECK_EQ(previous_tokens == nullptr, pending_tokens == nullptr);
        DCHECK(pending_tokens == nullptr || pending_tokens->empty());
        previous_object_tokens = previous_tokens;
        pending_object_tokens = pending_tokens;
        root_node = INVALID_INDEX;
        state = State::COLLECTING;
    }

    void rollback(const Checkpoint& start) noexcept {
        DCHECK_LE(start.scalar_bytes, scalar_bytes.size());
        DCHECK_LE(start.nodes, nodes.size());
        DCHECK_LE(start.containers, containers.size());
        DCHECK_LE(start.children, children.size());
        DCHECK_LE(start.key_references, key_references.size());
        scalar_bytes.resize(start.scalar_bytes);
        nodes.resize(start.nodes);
        containers.erase(containers.begin() + start.containers, containers.end());
        children.resize(start.children);
        key_references.resize(start.key_references);
        scope_stack.clear();
        active_container = INVALID_INDEX;
        object_id_scratch.clear();
        planned_object_children.clear();
        if (pending_object_tokens != nullptr) {
            pending_object_tokens->clear();
        }
        previous_object_tokens = nullptr;
        pending_object_tokens = nullptr;
        root_node = INVALID_INDEX;
        state = State::ABORTED;
    }

    void publish_object_links() noexcept {
        if (pending_object_tokens == nullptr) {
            return;
        }
        DCHECK(previous_object_tokens != nullptr);
        DCHECK_NE(root_node, INVALID_INDEX);
        previous_object_tokens->swap(*pending_object_tokens);
        pending_object_tokens->clear();
        previous_object_tokens = nullptr;
        pending_object_tokens = nullptr;
    }

    void discard_key_references(size_t begin) noexcept {
        DCHECK_LE(begin, key_references.size());
        key_references.resize(begin);
    }

    const uint32_t* key_reference_data(size_t begin) const noexcept {
        DCHECK_LE(begin, key_references.size());
        return key_references.empty() ? nullptr : key_references.data() + begin;
    }

    size_t key_reference_count(size_t begin) const noexcept {
        DCHECK_LE(begin, key_references.size());
        return key_references.size() - begin;
    }

    void ensure_collecting() const {
        if (state != State::COLLECTING) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "Cannot add to a Variant row builder after collection completes");
        }
        if (metadata.is_sealed()) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "Cannot add a Variant value after metadata seal");
        }
    }

    void ensure_can_add_value() const {
        ensure_collecting();
        if (active_container == INVALID_INDEX) {
            if (root_node != INVALID_INDEX) {
                throw Exception(ErrorCode::INVALID_ARGUMENT,
                                "Variant row builder accepts exactly one root value");
            }
            return;
        }

        DCHECK_LT(active_container, containers.size());
        const Container& parent = containers[active_container];
        if (parent.child_count == std::numeric_limits<uint32_t>::max()) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "Variant container exceeds the uint32 element limit");
        }
        if (children.size() == std::numeric_limits<uint32_t>::max()) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "Variant encoding unit exceeds the uint32 temporary child limit");
        }
        if (parent.kind == NodeKind::OBJECT && !parent.has_pending_field) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "Variant object value must be preceded by add_key");
        }
    }

    void prepare_value_node() const {
        ensure_can_add_value();
        if (nodes.size() == std::numeric_limits<uint32_t>::max()) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "Variant encoding unit exceeds the uint32 temporary node limit");
        }
    }

    uint32_t prepare_arena_scalar(size_t encoded_size) const {
        prepare_value_node();
        if (encoded_size > std::numeric_limits<uint32_t>::max() - scalar_bytes.size()) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "Variant encoding unit scalar scratch exceeds the uint32 byte limit");
        }
        return static_cast<uint32_t>(scalar_bytes.size());
    }

    void add_scalar(const VariantScalarEncodingPlan& plan) {
        if (plan.size() <= INLINE_SCALAR_CAPACITY) {
            prepare_value_node();
            std::array<char, INLINE_SCALAR_CAPACITY> inline_bytes {};
            plan.write(inline_bytes.data(), plan.size());
            complete_inline_scalar(inline_bytes, plan.size());
            return;
        }

        const uint32_t offset = prepare_arena_scalar(plan.size());
        scalar_bytes.resize(scalar_bytes.size() + plan.size());
        plan.write(scalar_bytes.data() + offset, plan.size());
        complete_scalar(offset);
    }

    void add_null() { add_scalar(VariantScalarEncodingPlan::null_value()); }

    void add_bool(bool value) { add_scalar(VariantScalarEncodingPlan::boolean(value)); }

    void add_int(int64_t value) { add_scalar(VariantScalarEncodingPlan::integer(value)); }

    void add_value(VariantValueRef value) {
        ensure_can_add_value();
        planned_object_children.clear();
        try {
            validate_import_root(value);
            if (scope_stack.size() > VARIANT_MAX_NESTING_DEPTH) {
                throw Exception(ErrorCode::INVALID_ARGUMENT,
                                "Variant import exceeds maximum nesting depth {}",
                                VARIANT_MAX_NESTING_DEPTH);
            }
            import_node(value, static_cast<uint32_t>(scope_stack.size()));
        } catch (...) {
            // The caller still owns row rollback; only release importer scratch here.
            planned_object_children.clear();
            throw;
        }
        DCHECK(planned_object_children.empty());
    }

    void import_primitive(VariantValueRef value) {
        const VariantPrimitiveId id = value.primitive_id();
        switch (id) {
        case VariantPrimitiveId::NULL_VALUE:
            add_null();
            return;
        case VariantPrimitiveId::TRUE_VALUE:
        case VariantPrimitiveId::FALSE_VALUE:
            add_bool(value.get_bool());
            return;
        case VariantPrimitiveId::INT8:
        case VariantPrimitiveId::INT16:
        case VariantPrimitiveId::INT32:
        case VariantPrimitiveId::INT64:
            add_int(value.get_int());
            return;
        case VariantPrimitiveId::FLOAT:
            add_scalar(VariantScalarEncodingPlan::float32(value.get_float()));
            return;
        case VariantPrimitiveId::DOUBLE:
            add_scalar(VariantScalarEncodingPlan::float64(value.get_double()));
            return;
        case VariantPrimitiveId::DECIMAL4:
        case VariantPrimitiveId::DECIMAL8:
        case VariantPrimitiveId::DECIMAL16: {
            const VariantDecimal decimal = value.get_decimal();
            if (magnitude(decimal.unscaled) > MAX_DECIMAL38) {
                throw Exception(ErrorCode::CORRUPTION,
                                "Variant imported decimal exceeds precision 38");
            }
            add_scalar(VariantScalarEncodingPlan::decimal(decimal.unscaled, decimal.scale));
            return;
        }
        case VariantPrimitiveId::DATE:
            add_scalar(VariantScalarEncodingPlan::date(value.get_date()));
            return;
        case VariantPrimitiveId::TIMESTAMP_MICROS:
            add_scalar(VariantScalarEncodingPlan::timestamp_micros(value.get_timestamp_micros(),
                                                                   true));
            return;
        case VariantPrimitiveId::TIMESTAMP_NTZ_MICROS:
            add_scalar(VariantScalarEncodingPlan::timestamp_micros(value.get_timestamp_ntz_micros(),
                                                                   false));
            return;
        case VariantPrimitiveId::BINARY:
            add_scalar(VariantScalarEncodingPlan::binary(value.get_binary()));
            return;
        case VariantPrimitiveId::STRING: {
            const StringRef string = value.get_string();
            require_import_utf8(string, "string");
            add_scalar(VariantScalarEncodingPlan::string(string));
            return;
        }
        case VariantPrimitiveId::TIME_NTZ_MICROS:
            add_scalar(VariantScalarEncodingPlan::time_ntz_micros(value.get_time_ntz_micros()));
            return;
        case VariantPrimitiveId::TIMESTAMP_NANOS:
            add_scalar(
                    VariantScalarEncodingPlan::timestamp_nanos(value.get_timestamp_nanos(), true));
            return;
        case VariantPrimitiveId::TIMESTAMP_NTZ_NANOS:
            add_scalar(VariantScalarEncodingPlan::timestamp_nanos(value.get_timestamp_ntz_nanos(),
                                                                  false));
            return;
        case VariantPrimitiveId::UUID:
            add_scalar(VariantScalarEncodingPlan::uuid(value.get_uuid()));
            return;
        }
    }

    size_t object_values_offset(VariantValueRef value, uint32_t count) const noexcept {
        const uint8_t value_header =
                static_cast<uint8_t>(value.data[0]) >> VARIANT_VALUE_HEADER_SHIFT;
        const auto offset_width = static_cast<uint8_t>(
                ((value_header >> VARIANT_OBJECT_OFFSET_SIZE_SHIFT) & 0x03) + 1);
        const auto id_width =
                static_cast<uint8_t>(((value_header >> VARIANT_OBJECT_ID_SIZE_SHIFT) & 0x03) + 1);
        const uint8_t count_width = (value_header & VARIANT_OBJECT_LARGE_MASK) != 0
                                            ? sizeof(uint32_t)
                                            : sizeof(uint8_t);
        return 1 + count_width + static_cast<size_t>(count) * id_width +
               (static_cast<size_t>(count) + 1) * offset_width;
    }

    void validate_import_object(VariantValueRef value, uint32_t count) {
        const size_t values_offset = object_values_offset(value, count);
        DCHECK_LE(values_offset, value.size);
        const char* values_begin = value.data + values_offset;
        const auto values_size = static_cast<uint32_t>(value.size - values_offset);
        // Pass-2 planning is idle during collection, so reuse its retained capacity to validate
        // the source object's physical value intervals without adding a per-row scratch buffer.
        planned_object_children.clear();
        planned_object_children.reserve(count);

        StringRef previous_key;
        for (uint32_t index = 0; index < count; ++index) {
            uint32_t field_id = 0;
            const VariantValueRef child = value.object_value_at(index, &field_id);
            const StringRef key = value.metadata.key_at(field_id);
            require_import_utf8(key, "object key");
            if (index != 0 && previous_key.compare(key) >= 0) {
                throw Exception(ErrorCode::CORRUPTION,
                                "Variant imported object keys are not strictly byte-sorted at "
                                "field {}",
                                index);
            }
            previous_key = key;
            const auto begin = static_cast<uint32_t>(child.data - values_begin);
            DCHECK_LE(child.size, std::numeric_limits<uint32_t>::max());
            planned_object_children.push_back(
                    {.node_index = begin, .final_field_id = static_cast<uint32_t>(child.size)});
        }

        sort_object_entries(planned_object_children, 0, planned_object_children.size(),
                            [](const FinalChild& left, const FinalChild& right) {
                                return left.node_index < right.node_index;
                            });
        uint32_t expected_begin = 0;
        for (const FinalChild& interval : planned_object_children) {
            if (interval.node_index != expected_begin) {
                throw Exception(ErrorCode::CORRUPTION,
                                "Variant imported object values are not a complete non-overlapping "
                                "partition at byte {}",
                                expected_begin);
            }
            expected_begin += interval.final_field_id;
        }
        if (expected_begin != values_size) {
            throw Exception(ErrorCode::CORRUPTION,
                            "Variant imported object values contain {} unreferenced trailing bytes",
                            values_size - expected_begin);
        }
        planned_object_children.clear();
    }

    void import_object(VariantValueRef value, uint32_t depth) {
        const uint32_t count = value.num_elements();
        validate_import_object(value, count);
        const uint32_t token = start_container(NodeKind::OBJECT);
        for (uint32_t index = 0; index < count; ++index) {
            uint32_t field_id = 0;
            const VariantValueRef child = value.object_value_at(index, &field_id);
            add_key(token, value.metadata.key_at(field_id));
            import_node(child, depth + 1);
        }
        finish_container(token, NodeKind::OBJECT);
    }

    void import_array(VariantValueRef value, uint32_t depth) {
        const uint32_t count = value.num_elements();
        const uint32_t token = start_container(NodeKind::ARRAY);
        for (uint32_t index = 0; index < count; ++index) {
            import_node(value.array_at(index), depth + 1);
        }
        finish_container(token, NodeKind::ARRAY);
    }

    void import_node(VariantValueRef value, uint32_t depth) {
        require_import_depth(depth);
        require_exact_import_value(value);
        switch (value.basic_type()) {
        case VariantBasicType::PRIMITIVE:
            import_primitive(value);
            return;
        case VariantBasicType::SHORT_STRING: {
            const StringRef string = value.get_string();
            require_import_utf8(string, "string");
            add_scalar(VariantScalarEncodingPlan::string(string));
            return;
        }
        case VariantBasicType::OBJECT:
            import_object(value, depth);
            return;
        case VariantBasicType::ARRAY:
            import_array(value, depth);
            return;
        }
    }

    void attach_node(uint32_t node_index) {
        if (active_container == INVALID_INDEX) {
            DCHECK_EQ(root_node, INVALID_INDEX);
            root_node = node_index;
            if (nodes[node_index].kind == NodeKind::INLINE_SCALAR ||
                nodes[node_index].kind == NodeKind::SCALAR) {
                state = State::COLLECTED;
            }
            return;
        }

        DCHECK_LT(active_container, containers.size());
        Container& parent = containers[active_container];
        const uint32_t field_id = parent.kind == NodeKind::OBJECT ? parent.pending_field_id : 0;
        const auto child_index = static_cast<uint32_t>(children.size());
        children.push_back({node_index, field_id, INVALID_INDEX});
        if (parent.first_child == INVALID_INDEX) {
            parent.first_child = child_index;
        } else {
            DCHECK_NE(parent.last_child, INVALID_INDEX);
            children[parent.last_child].next = child_index;
        }
        parent.last_child = child_index;
        ++parent.child_count;
        parent.has_pending_field = false;
    }

    void complete_scalar(uint32_t offset) {
        DCHECK_LE(scalar_bytes.size() - offset, std::numeric_limits<uint32_t>::max());
        const auto node_index = static_cast<uint32_t>(nodes.size());
        nodes.emplace_back(NodeKind::SCALAR, offset,
                           static_cast<uint32_t>(scalar_bytes.size() - offset));
        attach_node(node_index);
    }

    void complete_inline_scalar(const std::array<char, INLINE_SCALAR_CAPACITY>& inline_bytes,
                                size_t encoded_size) {
        DCHECK_GT(encoded_size, 0);
        DCHECK_LE(encoded_size, INLINE_SCALAR_CAPACITY);
        const auto node_index = static_cast<uint32_t>(nodes.size());
        nodes.emplace_back(inline_bytes, static_cast<uint32_t>(encoded_size));
        attach_node(node_index);
    }

    void add_inline_scalar(const std::array<char, INLINE_SCALAR_CAPACITY>& inline_bytes,
                           size_t encoded_size) {
        prepare_value_node();
        complete_inline_scalar(inline_bytes, encoded_size);
    }

    uint32_t start_container(NodeKind kind) {
        prepare_value_node();
        if (containers.size() == std::numeric_limits<uint32_t>::max()) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "Variant encoding unit exceeds the uint32 temporary container limit");
        }

        const auto token = static_cast<uint32_t>(containers.size());
        uint32_t previous_token = INVALID_INDEX;
        uint32_t previous_child_cursor = INVALID_INDEX;
        if (kind == NodeKind::OBJECT && pending_object_tokens != nullptr) {
            DCHECK(previous_object_tokens != nullptr);
            const size_t object_ordinal = pending_object_tokens->size();
            if (object_ordinal < previous_object_tokens->size()) {
                previous_token = (*previous_object_tokens)[object_ordinal];
                DCHECK_LT(previous_token, token);
                DCHECK(containers[previous_token].kind == NodeKind::OBJECT);
                previous_child_cursor = containers[previous_token].first_child;
            }
#ifdef BE_TEST
            const size_t old_capacity = pending_object_tokens->capacity();
#endif
            try {
                pending_object_tokens->push_back(token);
            } catch (...) {
                state = State::ABORTED;
                throw;
            }
#ifdef BE_TEST
            object_token_capacity_growths += old_capacity != pending_object_tokens->capacity();
#endif
        }
        try {
            containers.emplace_back(kind);
            containers.back().previous_or_source_token = previous_token;
            containers.back().previous_child_cursor = previous_child_cursor;
            const auto node_index = static_cast<uint32_t>(nodes.size());
            nodes.emplace_back(kind, token, 0);
            attach_node(node_index);
            scope_stack.push_back(token);
            active_container = token;
        } catch (...) {
            if (kind == NodeKind::OBJECT && pending_object_tokens != nullptr) {
                DCHECK(!pending_object_tokens->empty());
                DCHECK_EQ(pending_object_tokens->back(), token);
                pending_object_tokens->pop_back();
            }
            if (pending_object_tokens != nullptr) {
                // The active block row owns rollback; keep the state aborted so it releases its
                // metadata references before the next row begins.
                state = State::ABORTED;
            }
            throw;
        }
        return token;
    }

    void ensure_can_add_key(uint32_t token) const {
        ensure_collecting();
        if (token >= containers.size() || active_container != token ||
            containers[token].kind != NodeKind::OBJECT) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "Variant object scope is not the active scope");
        }
        if (containers[token].has_pending_field) {
            throw Exception(ErrorCode::INVALID_ARGUMENT, "Variant object key is missing its value");
        }
    }

    void commit_key(uint32_t token, uint32_t temporary_id) noexcept {
        DCHECK_LT(token, containers.size());
        Container& object = containers[token];
        DCHECK(object.kind == NodeKind::OBJECT);
        DCHECK(!object.has_pending_field);
        object.pending_field_id = temporary_id;
        object.has_pending_field = true;
    }

    void add_key(uint32_t token, StringRef key) {
        ensure_can_add_key(token);
        validate_string_ref(key, "metadata key");
        Container& object = containers[token];

        bool key_hit = false;
        uint32_t next_previous_child = INVALID_INDEX;
        uint32_t temporary_id = INVALID_INDEX;
        if (object.previous_or_source_token != INVALID_INDEX &&
            object.previous_child_cursor != INVALID_INDEX) {
            const Child& previous_child = children[object.previous_child_cursor];
            const StringRef previous_key =
                    metadata._temporary_key(previous_child.temporary_field_id);
            if (previous_key.size == key.size &&
                (key.size == 0 || std::memcmp(previous_key.data, key.data, key.size) == 0)) {
                key_hit = true;
                next_previous_child = previous_child.next;
                temporary_id = previous_child.temporary_field_id;
            }
        }
        if (!key_hit) {
            temporary_id = metadata.register_key(key);
        }

        key_references.push_back(temporary_id);
        metadata._retain_key(temporary_id);
        commit_key(token, temporary_id);
        if (key_hit) {
            object.previous_child_cursor = next_previous_child;
        } else {
            object.previous_or_source_token = INVALID_INDEX;
            object.previous_child_cursor = INVALID_INDEX;
        }
    }

    void validate_unique_object_keys(const Container& object) {
        object_id_scratch.clear();
        object_id_scratch.reserve(object.child_count);
        uint32_t child_index = object.first_child;
        for (uint32_t index = 0; index < object.child_count; ++index) {
            DCHECK_NE(child_index, INVALID_INDEX);
            const Child& child = children[child_index];
            object_id_scratch.push_back(child.temporary_field_id);
            child_index = child.next;
        }
        DCHECK_EQ(child_index, INVALID_INDEX);
        sort_object_entries(object_id_scratch, 0, object_id_scratch.size(),
                            [](uint32_t left, uint32_t right) { return left < right; });
        for (size_t index = 1; index < object_id_scratch.size(); ++index) {
            if (object_id_scratch[index - 1] == object_id_scratch[index]) {
                throw Exception(ErrorCode::INVALID_ARGUMENT, "Duplicate Variant object key");
            }
        }
    }

    void finish_container(uint32_t token, NodeKind expected_kind) {
        ensure_collecting();
        if (token >= containers.size() || active_container != token ||
            containers[token].kind != expected_kind) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "Variant container scopes must finish in nesting order");
        }
        if (expected_kind == NodeKind::OBJECT && containers[token].has_pending_field) {
            throw Exception(ErrorCode::INVALID_ARGUMENT, "Variant object key is missing its value");
        }
        if (expected_kind == NodeKind::OBJECT) {
            Container& object = containers[token];
            if (object.previous_or_source_token != INVALID_INDEX &&
                object.previous_child_cursor == INVALID_INDEX) {
                DCHECK_LT(object.previous_or_source_token, token);
                DCHECK_EQ(containers[object.previous_or_source_token].child_count,
                          object.child_count);
#ifdef BE_TEST
                ++object_schema_hits;
#endif
            } else {
                validate_unique_object_keys(object);
                object.previous_or_source_token = token;
#ifdef BE_TEST
                ++object_schema_fallbacks;
#endif
            }
        }
        DCHECK(!scope_stack.empty());
        DCHECK_EQ(scope_stack.back(), token);
        scope_stack.pop_back();
        if (scope_stack.empty()) {
            active_container = INVALID_INDEX;
            state = State::COLLECTED;
        } else {
            active_container = scope_stack.back();
        }
    }

    void sort_object_children(size_t begin, size_t count) {
        const auto less_by_id = [](const FinalChild& left, const FinalChild& right) {
            return left.final_field_id < right.final_field_id;
        };
        sort_object_entries(planned_object_children, begin, count, less_by_id);
        for (size_t index = begin + 1; index < begin + count; ++index) {
            DCHECK_LT(planned_object_children[index - 1].final_field_id,
                      planned_object_children[index].final_field_id);
        }
    }

    // Recursive pass-2 is a hot path; splitting it adds call layers in ASAN and production.
    // NOLINTNEXTLINE(readability-function-cognitive-complexity,readability-function-size)
    size_t plan_node(uint32_t node_index) {
        DCHECK_LT(node_index, nodes.size());
        const Node& node = nodes[node_index];
        if (node.kind == NodeKind::INLINE_SCALAR || node.kind == NodeKind::SCALAR) {
            return node.payload_size;
        }

        const Container& container = containers[node.payload_index];
        ContainerPlan& plan = container_plans[node.payload_index];
        const uint32_t count = container.child_count;
        plan.count_width =
                count > std::numeric_limits<uint8_t>::max() ? sizeof(uint32_t) : sizeof(uint8_t);

        if (node.kind == NodeKind::OBJECT) {
            plan.object_children_begin = planned_object_children.size();
            DCHECK_NE(container.previous_or_source_token, INVALID_INDEX);
            if (container.previous_or_source_token != node.payload_index) {
                const uint32_t source_token = container.previous_or_source_token;
                DCHECK_LT(source_token, node.payload_index);
                const Container& source = containers[source_token];
                const ContainerPlan& source_plan = container_plans[source_token];
                DCHECK(source.kind == NodeKind::OBJECT);
                DCHECK_EQ(source.child_count, count);
                DCHECK_GT(source_plan.encoded_size, 0);

                uint32_t child_index = container.first_child;
                for (uint32_t index = 0; index < count; ++index) {
                    DCHECK_NE(child_index, INVALID_INDEX);
                    const Child& child = children[child_index];
                    const uint32_t final_id = metadata.final_id(child.temporary_field_id);
                    DCHECK_LT(final_id, object_id_scratch.size());
                    DCHECK_EQ(object_id_scratch[final_id], INVALID_INDEX);
                    object_id_scratch[final_id] = child.node_index;
                    child_index = child.next;
                }
                DCHECK_EQ(child_index, INVALID_INDEX);

                for (uint32_t index = 0; index < count; ++index) {
                    const uint32_t final_id =
                            planned_object_children[source_plan.object_children_begin + index]
                                    .final_field_id;
                    DCHECK_LT(final_id, object_id_scratch.size());
                    const uint32_t current_node = object_id_scratch[final_id];
                    DCHECK_NE(current_node, INVALID_INDEX);
                    planned_object_children.push_back({current_node, final_id});
                    object_id_scratch[final_id] = INVALID_INDEX;
                }
#ifdef BE_TEST
                ++object_plan_reuses;
#endif
            } else {
                uint32_t child_index = container.first_child;
                for (uint32_t index = 0; index < count; ++index) {
                    DCHECK_NE(child_index, INVALID_INDEX);
                    const Child& child = children[child_index];
                    planned_object_children.push_back(
                            {child.node_index, metadata.final_id(child.temporary_field_id)});
                    child_index = child.next;
                }
                DCHECK_EQ(child_index, INVALID_INDEX);
                sort_object_children(plan.object_children_begin, count);
#ifdef BE_TEST
                ++object_plan_fallbacks;
#endif
            }
        }

        uint32_t values_size = 0;
        uint32_t array_child_index = container.first_child;
        for (uint32_t index = 0; index < count; ++index) {
            uint32_t child_node = 0;
            if (node.kind == NodeKind::OBJECT) {
                child_node = planned_object_children[plan.object_children_begin + index].node_index;
            } else {
                DCHECK_NE(array_child_index, INVALID_INDEX);
                child_node = children[array_child_index].node_index;
                array_child_index = children[array_child_index].next;
            }
            const size_t child_size = plan_node(child_node);
            if (child_size > std::numeric_limits<uint32_t>::max() - values_size) {
                throw Exception(ErrorCode::INVALID_ARGUMENT,
                                "Variant container values exceed the uint32 byte limit");
            }
            values_size += static_cast<uint32_t>(child_size);
        }
        if (node.kind == NodeKind::ARRAY) {
            DCHECK_EQ(array_child_index, INVALID_INDEX);
        }
        plan.values_size = values_size;
        plan.offset_width = minimum_unsigned_width(values_size);
        if (node.kind == NodeKind::OBJECT) {
            const uint32_t maximum_id =
                    count == 0 ? 0
                               : planned_object_children[plan.object_children_begin + count - 1]
                                         .final_field_id;
            plan.id_width = minimum_unsigned_width(maximum_id);
        }

        uint64_t encoded_size = 1 + plan.count_width + values_size;
        encoded_size += (static_cast<uint64_t>(count) + 1) * plan.offset_width;
        if (node.kind == NodeKind::OBJECT) {
            encoded_size += static_cast<uint64_t>(count) * plan.id_width;
        }
        if (encoded_size > std::numeric_limits<size_t>::max()) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "Variant row exceeds the addressable output size");
        }
        plan.encoded_size = static_cast<size_t>(encoded_size);
        return plan.encoded_size;
    }

    void reset_plan() {
        container_plans.assign(containers.size(), {});
        planned_object_children.clear();
        size_t object_child_count = 0;
        bool has_object_plan_reuse = false;
        for (size_t index = 0; index < containers.size(); ++index) {
            const Container& container = containers[index];
            if (container.kind != NodeKind::OBJECT) {
                continue;
            }
            DCHECK_NE(container.previous_or_source_token, INVALID_INDEX);
            has_object_plan_reuse |= container.previous_or_source_token != index;
            if (container.child_count > planned_object_children.max_size() - object_child_count) {
                throw Exception(ErrorCode::INVALID_ARGUMENT,
                                "Variant object plan exceeds the addressable size");
            }
            object_child_count += container.child_count;
        }
        planned_object_children.reserve(object_child_count);
        if (has_object_plan_reuse) {
            object_id_scratch.assign(metadata.num_keys(), INVALID_INDEX);
        } else {
            object_id_scratch.clear();
        }
    }

    size_t prepare_plan(uint32_t root_index) { return plan_node(root_index); }

    size_t node_encoded_size(uint32_t node_index) const noexcept {
        DCHECK_LT(node_index, nodes.size());
        const Node& node = nodes[node_index];
        return node.kind == NodeKind::INLINE_SCALAR || node.kind == NodeKind::SCALAR
                       ? node.payload_size
                       : container_plans[node.payload_index].encoded_size;
    }

    void write_node(uint32_t node_index, char*& output) const noexcept {
        DCHECK_LT(node_index, nodes.size());
        const Node& node = nodes[node_index];
        if (node.kind == NodeKind::INLINE_SCALAR) {
            std::memcpy(output, node.inline_bytes.data(), node.payload_size);
            output += node.payload_size;
            return;
        }
        if (node.kind == NodeKind::SCALAR) {
            std::memcpy(output, scalar_bytes.data() + node.payload_index, node.payload_size);
            output += node.payload_size;
            return;
        }

        const Container& container = containers[node.payload_index];
        const ContainerPlan& plan = container_plans[node.payload_index];
        const uint32_t count = container.child_count;
        const bool is_large = plan.count_width == sizeof(uint32_t);
        if (node.kind == NodeKind::OBJECT) {
            const auto value_header = static_cast<uint8_t>(
                    ((plan.offset_width - 1) << VARIANT_OBJECT_OFFSET_SIZE_SHIFT) |
                    ((plan.id_width - 1) << VARIANT_OBJECT_ID_SIZE_SHIFT) |
                    (is_large ? VARIANT_OBJECT_LARGE_MASK : 0));
            *output++ = static_cast<char>((value_header << VARIANT_VALUE_HEADER_SHIFT) |
                                          static_cast<uint8_t>(VariantBasicType::OBJECT));
            write_unsigned(output, count, plan.count_width);
            for (uint32_t index = 0; index < count; ++index) {
                write_unsigned(
                        output,
                        planned_object_children[plan.object_children_begin + index].final_field_id,
                        plan.id_width);
            }
        } else {
            const auto value_header = static_cast<uint8_t>(
                    ((plan.offset_width - 1) << VARIANT_ARRAY_OFFSET_SIZE_SHIFT) |
                    (is_large ? VARIANT_ARRAY_LARGE_MASK : 0));
            *output++ = static_cast<char>((value_header << VARIANT_VALUE_HEADER_SHIFT) |
                                          static_cast<uint8_t>(VariantBasicType::ARRAY));
            write_unsigned(output, count, plan.count_width);
        }

        uint32_t offset = 0;
        write_unsigned(output, offset, plan.offset_width);
        uint32_t array_child_index = container.first_child;
        for (uint32_t index = 0; index < count; ++index) {
            uint32_t child_node = 0;
            if (node.kind == NodeKind::OBJECT) {
                child_node = planned_object_children[plan.object_children_begin + index].node_index;
            } else {
                DCHECK_NE(array_child_index, INVALID_INDEX);
                child_node = children[array_child_index].node_index;
                array_child_index = children[array_child_index].next;
            }
            offset += static_cast<uint32_t>(node_encoded_size(child_node));
            write_unsigned(output, offset, plan.offset_width);
        }
        DCHECK_EQ(offset, plan.values_size);
        if (node.kind == NodeKind::ARRAY) {
            DCHECK_EQ(array_child_index, INVALID_INDEX);
        }

        array_child_index = container.first_child;
        for (uint32_t index = 0; index < count; ++index) {
            uint32_t child_node = 0;
            if (node.kind == NodeKind::OBJECT) {
                child_node = planned_object_children[plan.object_children_begin + index].node_index;
            } else {
                DCHECK_NE(array_child_index, INVALID_INDEX);
                child_node = children[array_child_index].node_index;
                array_child_index = children[array_child_index].next;
            }
            write_node(child_node, output);
        }
        if (node.kind == NodeKind::ARRAY) {
            DCHECK_EQ(array_child_index, INVALID_INDEX);
        }
    }

    void release_scratch() noexcept {
        release_variant_tracked_container(scalar_bytes);
        release_variant_tracked_container(nodes);
        release_variant_tracked_container(containers);
        release_variant_tracked_container(children);
        release_variant_tracked_container(scope_stack);
        release_variant_tracked_container(object_id_scratch);
        release_variant_tracked_container(key_references);
        release_variant_tracked_container(container_plans);
        release_variant_tracked_container(planned_object_children);
        previous_object_tokens = nullptr;
        pending_object_tokens = nullptr;
        active_container = INVALID_INDEX;
        root_node = INVALID_INDEX;
    }

    VariantMetadataBuilder& metadata;
    DorisVector<char> scalar_bytes;
    DorisVector<Node> nodes;
    DorisVector<Container> containers;
    DorisVector<Child> children;
    DorisVector<uint32_t> scope_stack;
    DorisVector<uint32_t> object_id_scratch;
    DorisVector<uint32_t> key_references;
    DorisVector<ContainerPlan> container_plans;
    DorisVector<FinalChild> planned_object_children;
    DorisVector<uint32_t>* previous_object_tokens = nullptr;
    DorisVector<uint32_t>* pending_object_tokens = nullptr;
#ifdef BE_TEST
    size_t object_token_capacity_growths = 0;
    size_t object_schema_hits = 0;
    size_t object_schema_fallbacks = 0;
    size_t object_plan_reuses = 0;
    size_t object_plan_fallbacks = 0;
#endif
    uint32_t active_container = INVALID_INDEX;
    uint32_t root_node = INVALID_INDEX;
    State state = State::COLLECTING;
};

struct VariantBlockBuilder::Impl {
    Impl() = default;

    void reserve(const ReserveHint& hint) {
        collection.reserve(hint.scalar_bytes, hint.nodes, hint.containers, hint.children);
        roots.reserve(hint.rows);
        previous_object_tokens.reserve(hint.containers);
        pending_object_tokens.reserve(hint.containers);
    }

    void ensure_open() const {
        if (terminal) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "Variant block builder is already finished");
        }
    }

    void ensure_active(uint64_t generation) const {
        ensure_open();
        if (!row_active || generation != active_generation) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "Variant block row handle is not the active row");
        }
    }

#ifdef BE_TEST
    void observe_collection_growth(const VariantCollectionCore::CapacitySnapshot& before) noexcept {
        const VariantCollectionCore::CapacitySnapshot after = collection.capacity_snapshot();
        counters.scalar_capacity_growths += before.scalar_bytes != after.scalar_bytes;
        counters.node_capacity_growths += before.nodes != after.nodes;
        counters.container_capacity_growths += before.containers != after.containers;
        counters.child_capacity_growths += before.children != after.children;
        counters.scope_stack_capacity_growths += before.scope_stack != after.scope_stack;
        counters.object_id_scratch_capacity_growths +=
                before.object_id_scratch != after.object_id_scratch;
        counters.key_reference_capacity_growths += before.key_references != after.key_references;
        counters.container_plan_capacity_growths += before.container_plans != after.container_plans;
        counters.planned_object_child_capacity_growths +=
                before.planned_object_children != after.planned_object_children;
    }

    void observe_root_growth(size_t before) {
        counters.row_root_capacity_growths += before != roots.capacity();
    }

    void refresh_counters(size_t metadata_capacity, size_t metadata_growths,
                          size_t metadata_unique_keys) const noexcept {
        const VariantCollectionCore::CapacitySnapshot capacity = collection.capacity_snapshot();
        counters.metadata_capacity_growths = metadata_growths;
        counters.metadata_unique_keys = metadata_unique_keys;
        counters.metadata_key_capacity = metadata_capacity;
        counters.scalar_byte_capacity = capacity.scalar_bytes;
        counters.node_capacity = capacity.nodes;
        counters.container_capacity = capacity.containers;
        counters.child_capacity = capacity.children;
        counters.scope_stack_capacity = capacity.scope_stack;
        counters.object_id_scratch_capacity = capacity.object_id_scratch;
        counters.key_reference_capacity = capacity.key_references;
        counters.container_plan_capacity = capacity.container_plans;
        counters.planned_object_child_capacity = capacity.planned_object_children;
        counters.row_root_capacity = roots.capacity();
        counters.object_token_capacity_growths = collection.object_token_capacity_growths;
        counters.object_schema_hits = collection.object_schema_hits;
        counters.object_schema_fallbacks = collection.object_schema_fallbacks;
        counters.object_plan_reuses = collection.object_plan_reuses;
        counters.object_plan_fallbacks = collection.object_plan_fallbacks;
        counters.previous_object_token_capacity = previous_object_tokens.capacity();
        counters.pending_object_token_capacity = pending_object_tokens.capacity();
    }

    mutable TestCounters counters;
    VariantCollectionCore::CapacitySnapshot row_capacity_start {};
#endif

    VariantMetadataBuilder metadata;
    VariantCollectionCore collection {metadata};
    DorisVector<uint32_t> roots;
    DorisVector<uint32_t> previous_object_tokens;
    DorisVector<uint32_t> pending_object_tokens;
    VariantCollectionCore::Checkpoint row_start {};
    uint64_t current_generation = 0;
    uint64_t active_generation = 0;
    bool row_active = false;
    bool terminal = false;
#ifdef BE_TEST
    bool counters_finalized = false;
#endif
};

VariantBlockBuilder::VariantBlockBuilder() : VariantBlockBuilder(ReserveHint {}) {}

VariantBlockBuilder::VariantBlockBuilder(ReserveHint hint) : _impl(std::make_unique<Impl>()) {
    _impl->metadata._reserve_keys(hint.metadata_keys);
    _impl->reserve(hint);
#ifdef BE_TEST
    _impl->refresh_counters(_impl->metadata._key_capacity(),
                            _impl->metadata._key_capacity_growths(), _impl->metadata.num_keys());
#endif
}

VariantBlockBuilder::~VariantBlockBuilder() {
    if (_impl != nullptr && _impl->row_active) {
        _abort_row_noexcept(_impl->active_generation);
    }
}

void VariantBlockBuilder::_add_scalar(uint64_t generation, const VariantScalarEncodingPlan& plan) {
    _impl->ensure_active(generation);
    _impl->collection.add_scalar(plan);
}

void VariantBlockBuilder::_add_null(uint64_t generation) {
    _impl->ensure_active(generation);
    _impl->collection.add_null();
}

void VariantBlockBuilder::_add_bool(uint64_t generation, bool value) {
    _impl->ensure_active(generation);
    _impl->collection.add_bool(value);
}

void VariantBlockBuilder::_add_int(uint64_t generation, int64_t value) {
    _impl->ensure_active(generation);
    _impl->collection.add_int(value);
}

void VariantBlockBuilder::_add_value(uint64_t generation, VariantValueRef value) {
    _impl->ensure_active(generation);
    _impl->collection.add_value(value);
}

uint32_t VariantBlockBuilder::_start_container(uint64_t generation, bool object) {
    _impl->ensure_active(generation);
    return _impl->collection.start_container(object ? VariantCollectionCore::NodeKind::OBJECT
                                                    : VariantCollectionCore::NodeKind::ARRAY);
}

void VariantBlockBuilder::_add_key(uint64_t generation, uint32_t token, StringRef key) {
    _impl->ensure_active(generation);
    _impl->collection.add_key(token, key);
}

void VariantBlockBuilder::_finish_container(uint64_t generation, uint32_t token, bool object) {
    _impl->ensure_active(generation);
    _impl->collection.finish_container(token, object ? VariantCollectionCore::NodeKind::OBJECT
                                                     : VariantCollectionCore::NodeKind::ARRAY);
}

void VariantBlockBuilder::_finish_row(uint64_t generation) {
    _impl->ensure_active(generation);
    if (_impl->collection.state == VariantCollectionCore::State::COLLECTING) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "Cannot finish an incomplete Variant row");
    }
    DCHECK(_impl->collection.state == VariantCollectionCore::State::COLLECTED);
#ifdef BE_TEST
    const size_t root_capacity = _impl->roots.capacity();
#endif
    _impl->roots.push_back(_impl->collection.root_node);
#ifdef BE_TEST
    _impl->observe_root_growth(root_capacity);
#endif
    _impl->metadata._complete_row();
    _impl->collection.discard_key_references(_impl->row_start.key_references);
    _impl->collection.publish_object_links();
    _impl->collection.state = VariantCollectionCore::State::FINISHED;
#ifdef BE_TEST
    _impl->observe_collection_growth(_impl->row_capacity_start);
#endif
    _impl->row_active = false;
}

void VariantBlockBuilder::_abort_row_noexcept(uint64_t generation) noexcept {
    if (!_impl->row_active || generation != _impl->active_generation) {
        return;
    }
    _impl->metadata._abort_row(
            _impl->collection.key_reference_data(_impl->row_start.key_references),
            _impl->collection.key_reference_count(_impl->row_start.key_references), true);
    _impl->collection.rollback(_impl->row_start);
#ifdef BE_TEST
    _impl->observe_collection_growth(_impl->row_capacity_start);
#endif
    _impl->row_active = false;
}

void VariantBlockBuilder::_abort_row(uint64_t generation) {
    _impl->ensure_active(generation);
    _abort_row_noexcept(generation);
}

VariantBlockBuilder::Row VariantBlockBuilder::begin_row() {
    _impl->ensure_open();
    if (_impl->row_active) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant block builder already has an active row");
    }
    if (_impl->current_generation >= ABORTED_ROW_GENERATION - 1) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant block row generation exceeds the uint64 limit");
    }
    ++_impl->current_generation;
    DCHECK(_impl->pending_object_tokens.empty());
#ifdef BE_TEST
    _impl->row_capacity_start = _impl->collection.capacity_snapshot();
#endif
    _impl->collection.begin_collection(&_impl->previous_object_tokens,
                                       &_impl->pending_object_tokens);
    _impl->row_start = _impl->collection.checkpoint();
    _impl->metadata._begin_row();
    _impl->active_generation = _impl->current_generation;
    _impl->row_active = true;
    return {this, _impl->active_generation};
}

VariantEncodedBlock VariantBlockBuilder::finish_block() {
    _impl->ensure_open();
    if (_impl->row_active) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Cannot finish a Variant block while a row is active");
    }
    _impl->terminal = true;
    _impl->metadata.seal();
#ifdef BE_TEST
    const VariantCollectionCore::CapacitySnapshot before = _impl->collection.capacity_snapshot();
#endif
    _impl->collection.reset_plan();

    DorisVector<uint32_t> offsets;
    if (_impl->roots.size() == offsets.max_size()) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant block row offsets exceed the addressable size");
    }
    offsets.reserve(_impl->roots.size() + 1);
    offsets.push_back(0);
    uint32_t total_size = 0;
    for (uint32_t root : _impl->roots) {
        const size_t row_size = _impl->collection.prepare_plan(root);
        if (row_size > std::numeric_limits<uint32_t>::max() - total_size) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "Variant block values exceed the ColumnString uint32 byte limit");
        }
        total_size += static_cast<uint32_t>(row_size);
        offsets.push_back(total_size);
    }
#ifdef BE_TEST
    _impl->observe_collection_growth(before);
#endif
    auto storage = std::make_unique<VariantEncodedBlockStorage>();
    storage->offsets = std::move(offsets);
    storage->values.resize(total_size);
    char* output = storage->values.data();
    for (uint32_t root : _impl->roots) {
        _impl->collection.write_node(root, output);
    }
    DCHECK_EQ(output, storage->values.data() + storage->values.size());

#ifdef BE_TEST
    _impl->refresh_counters(_impl->metadata._key_capacity(),
                            _impl->metadata._key_capacity_growths(), _impl->metadata.num_keys());
    _impl->counters_finalized = true;
#endif
    _impl->metadata._move_encoded_metadata(*storage);
    _impl->collection.release_scratch();
    release_variant_tracked_container(_impl->roots);
    release_variant_tracked_container(_impl->previous_object_tokens);
    release_variant_tracked_container(_impl->pending_object_tokens);
    return VariantEncodedBlock(std::move(storage));
}

#ifdef BE_TEST
const VariantBlockBuilder::TestCounters& VariantBlockBuilder::test_counters() const noexcept {
    if (!_impl->counters_finalized) {
        _impl->refresh_counters(_impl->metadata._key_capacity(),
                                _impl->metadata._key_capacity_growths(),
                                _impl->metadata.num_keys());
    }
    return _impl->counters;
}
#endif

VariantBlockBuilder::Row::ObjectScope::ObjectScope(VariantBlockBuilder* builder,
                                                   uint64_t generation, uint32_t token) noexcept
        : _builder(builder), _generation(generation), _token(token) {}

VariantBlockBuilder::Row::ObjectScope::ObjectScope(ObjectScope&& other) noexcept
        : _builder(std::exchange(other._builder, nullptr)),
          _generation(other._generation),
          _token(other._token) {}

VariantBlockBuilder::Row::ArrayScope::ArrayScope(VariantBlockBuilder* builder, uint64_t generation,
                                                 uint32_t token) noexcept
        : _builder(builder), _generation(generation), _token(token) {}

VariantBlockBuilder::Row::ArrayScope::ArrayScope(ArrayScope&& other) noexcept
        : _builder(std::exchange(other._builder, nullptr)),
          _generation(other._generation),
          _token(other._token) {}

VariantBlockBuilder::Row::Row(VariantBlockBuilder* builder, uint64_t generation) noexcept
        : _builder(builder), _generation(generation) {}

VariantBlockBuilder::Row::Row(Row&& other) noexcept
        : _builder(std::exchange(other._builder, nullptr)), _generation(other._generation) {}

void VariantBlockBuilder::Row::ObjectScope::add_key(StringRef key) {
    if (_builder == nullptr) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant block object scope is already finished");
    }
    _builder->_add_key(_generation, _token, key);
}

void VariantBlockBuilder::Row::ObjectScope::finish() {
    if (_builder == nullptr) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant block object scope is already finished");
    }
    _builder->_finish_container(_generation, _token, true);
    _builder = nullptr;
}

void VariantBlockBuilder::Row::ArrayScope::finish() {
    if (_builder == nullptr) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant block array scope is already finished");
    }
    _builder->_finish_container(_generation, _token, false);
    _builder = nullptr;
}

VariantBlockBuilder::Row::~Row() {
    if (_builder != nullptr) {
        _builder->_abort_row_noexcept(_generation);
    }
}

void VariantBlockBuilder::Row::add_null() {
    if (_builder == nullptr) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "Variant block row handle is moved-from");
    }
    _builder->_add_null(_generation);
}

void VariantBlockBuilder::Row::add_bool(bool value) {
    if (_builder == nullptr) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "Variant block row handle is moved-from");
    }
    _builder->_add_bool(_generation, value);
}

void VariantBlockBuilder::Row::add_int(int64_t value) {
    if (_builder == nullptr) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "Variant block row handle is moved-from");
    }
    _builder->_add_int(_generation, value);
}

void VariantBlockBuilder::Row::add_float(float value) {
    _add_scalar(VariantScalarEncodingPlan::float32(value));
}

void VariantBlockBuilder::Row::add_double(double value) {
    _add_scalar(VariantScalarEncodingPlan::float64(value));
}

void VariantBlockBuilder::Row::add_decimal(__int128 unscaled, uint8_t scale) {
    _add_scalar(VariantScalarEncodingPlan::decimal(unscaled, scale));
}

void VariantBlockBuilder::Row::add_decimal(__int128 unscaled, uint8_t scale, uint8_t width) {
    _add_scalar(VariantScalarEncodingPlan::decimal(unscaled, scale, width));
}

void VariantBlockBuilder::Row::add_date(int32_t days_since_epoch) {
    _add_scalar(VariantScalarEncodingPlan::date(days_since_epoch));
}

void VariantBlockBuilder::Row::add_timestamp_micros(int64_t value, bool utc_adjusted) {
    _add_scalar(VariantScalarEncodingPlan::timestamp_micros(value, utc_adjusted));
}

void VariantBlockBuilder::Row::add_timestamp_nanos(int64_t value, bool utc_adjusted) {
    _add_scalar(VariantScalarEncodingPlan::timestamp_nanos(value, utc_adjusted));
}

void VariantBlockBuilder::Row::add_time_ntz_micros(int64_t value) {
    _add_scalar(VariantScalarEncodingPlan::time_ntz_micros(value));
}

void VariantBlockBuilder::Row::add_binary(StringRef value) {
    _add_scalar(VariantScalarEncodingPlan::binary(value));
}

void VariantBlockBuilder::Row::add_string(StringRef value) {
    _add_scalar(VariantScalarEncodingPlan::string(value));
}

void VariantBlockBuilder::Row::add_uuid(const std::array<uint8_t, 16>& value) {
    _add_scalar(VariantScalarEncodingPlan::uuid(value));
}

void VariantBlockBuilder::Row::add_largeint(__int128 value) {
    _add_scalar(VariantScalarEncodingPlan::largeint(value));
}

void VariantBlockBuilder::Row::add_value(VariantValueRef value) {
    if (_builder == nullptr) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "Variant block row handle is moved-from");
    }
    _builder->_add_value(_generation, value);
}

VariantBlockBuilder::Row::ObjectScope VariantBlockBuilder::Row::start_object() {
    const uint32_t token = _start_object();
    return {_builder, _generation, token};
}

VariantBlockBuilder::Row::ArrayScope VariantBlockBuilder::Row::start_array() {
    const uint32_t token = _start_array();
    return {_builder, _generation, token};
}

void VariantBlockBuilder::Row::finish() {
    if (_builder == nullptr) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "Variant block row handle is moved-from");
    }
    _builder->_finish_row(_generation);
    _generation = FINISHED_ROW_GENERATION;
}

void VariantBlockBuilder::Row::abort() {
    if (_builder == nullptr) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "Variant block row handle is moved-from");
    }
    _builder->_abort_row(_generation);
    _generation = ABORTED_ROW_GENERATION;
}

bool VariantBlockBuilder::Row::is_finished() const noexcept {
    return _builder != nullptr && _generation == FINISHED_ROW_GENERATION;
}

void VariantBlockBuilder::Row::_add_scalar(const VariantScalarEncodingPlan& plan) {
    if (_builder == nullptr) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "Variant block row handle is moved-from");
    }
    _builder->_add_scalar(_generation, plan);
}

uint32_t VariantBlockBuilder::Row::_start_object() {
    if (_builder == nullptr) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "Variant block row handle is moved-from");
    }
    return _builder->_start_container(_generation, true);
}

uint32_t VariantBlockBuilder::Row::_start_array() {
    if (_builder == nullptr) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "Variant block row handle is moved-from");
    }
    return _builder->_start_container(_generation, false);
}

} // namespace doris
