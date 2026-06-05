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

#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include "common/status.h"
#include "core/data_type/data_type.h"
#include "exprs/vexpr_fwd.h"

namespace doris::reader {

// File-local top-level column id.
//
// Scope:
// - Only valid inside one physical file schema returned by FileReader::get_schema().
// - For Parquet, this is the top-level field ordinal in the new reader schema.
// - The synthetic row-position column also uses this type, with a reserved negative id.
//
// Do not use this for table/global column unique ids, block positions, nested child ids, or
// slot ids. Nested child ids are carried by LocalColumnIndex::index below.
class LocalColumnId {
public:
    constexpr LocalColumnId() = default;
    explicit constexpr LocalColumnId(int32_t id) : _id(id) {}

    static constexpr LocalColumnId invalid() { return LocalColumnId(); }

    constexpr int32_t value() const { return _id; }
    constexpr bool is_valid() const { return _id >= 0; }

    constexpr bool operator==(const LocalColumnId& other) const { return _id == other._id; }
    constexpr bool operator!=(const LocalColumnId& other) const { return !(*this == other); }
    constexpr bool operator<(const LocalColumnId& other) const { return _id < other._id; }

private:
    int32_t _id = -1;
};

// Position of a file-local column in the Block produced by one FileScanRequest.
//
// This is assigned by TableColumnMapper/TableReader after predicate/non-predicate columns are
// deduplicated. It is not a file schema id and it is not stable across requests. Use value() only
// at the boundary where an existing Block or expression API still expects a size_t/int position.
class LocalIndex {
public:
    constexpr LocalIndex() = default;
    explicit constexpr LocalIndex(size_t index) : _index(index) {}

    constexpr size_t value() const { return _index; }
    constexpr bool operator==(const LocalIndex& other) const { return _index == other._index; }
    constexpr bool operator<(const LocalIndex& other) const { return _index < other._index; }

private:
    size_t _index = 0;
};

// Position of a table/global output column in the final Block returned by TableReader.
//
// This type is reserved for boundaries that need to refer to caller-visible column order. It must
// not be used to index a file-local Block, because schema evolution and lazy materialization can
// make file-local order different from table output order.
class GlobalIndex {
public:
    constexpr GlobalIndex() = default;
    explicit constexpr GlobalIndex(size_t index) : _index(index) {}

    constexpr size_t value() const { return _index; }
    constexpr bool operator==(const GlobalIndex& other) const { return _index == other._index; }
    constexpr bool operator<(const GlobalIndex& other) const { return _index < other._index; }

private:
    size_t _index = 0;
};

// Index of a split-local constant/default value used to materialize columns that are not read from
// the physical file, such as partition columns, added columns with default values, and virtual
// table-format columns.
//
// It is separate from LocalIndex because constants do not occupy a position in the file reader
// output block unless an expression explicitly materializes them.
class ConstantIndex {
public:
    constexpr ConstantIndex() = default;
    explicit constexpr ConstantIndex(size_t index) : _index(index) {}

    constexpr size_t value() const { return _index; }
    constexpr bool operator==(const ConstantIndex& other) const { return _index == other._index; }
    constexpr bool operator<(const ConstantIndex& other) const { return _index < other._index; }

private:
    size_t _index = 0;
};

inline std::ostream& operator<<(std::ostream& os, const LocalColumnId& id) {
    return os << id.value();
}

inline std::ostream& operator<<(std::ostream& os, const LocalIndex& index) {
    return os << index.value();
}

inline std::ostream& operator<<(std::ostream& os, const GlobalIndex& index) {
    return os << index.value();
}

inline std::ostream& operator<<(std::ostream& os, const ConstantIndex& index) {
    return os << index.value();
}

// A split/file-local constant value used to materialize a table/global column without reading a
// physical file column.
//
// Common producers are partition values, schema-evolution default expressions, generated columns
// and table-format virtual columns. The entry is keyed by ConstantIndex in ConstantMap; global_index
// keeps the link back to the caller-visible output column.
struct ConstantEntry {
    GlobalIndex global_index;
    VExprContextSPtr expr;
    DataTypePtr type;
};

// Per mapping/split collection of constants.
//
// ConstantIndex only has meaning within this container. Keeping constants separate from LocalIndex
// makes it explicit that these values do not occupy positions in the file reader output Block.
class ConstantMap {
public:
    ConstantIndex add(ConstantEntry entry) {
        const auto index = ConstantIndex(_entries.size());
        _entries.push_back(std::move(entry));
        return index;
    }

    const ConstantEntry& get(ConstantIndex index) const {
        DORIS_CHECK(index.value() < _entries.size());
        return _entries[index.value()];
    }

    void clear() { _entries.clear(); }
    bool empty() const { return _entries.empty(); }
    size_t size() const { return _entries.size(); }

    const std::vector<ConstantEntry>& entries() const { return _entries; }

private:
    std::vector<ConstantEntry> _entries;
};

// Target of a localized filter.
//
// A filter can either reference a file-local Block position or a constant entry. Unset entries mean
// the filter cannot be evaluated below the table-reader finalize stage.
struct FilterEntry {
    enum class Kind {
        UNSET,
        LOCAL,
        CONSTANT,
    };

    static FilterEntry local(LocalIndex index) {
        return {.kind = Kind::LOCAL, .index = index.value()};
    }

    static FilterEntry constant(ConstantIndex index) {
        return {.kind = Kind::CONSTANT, .index = index.value()};
    }

    bool is_set() const { return kind != Kind::UNSET; }
    bool is_local() const { return kind == Kind::LOCAL; }
    bool is_constant() const { return kind == Kind::CONSTANT; }

    LocalIndex local_index() const {
        DORIS_CHECK(is_local());
        return LocalIndex(index);
    }

    ConstantIndex constant_index() const {
        DORIS_CHECK(is_constant());
        return ConstantIndex(index);
    }

    Kind kind = Kind::UNSET;
    size_t index = 0;
};

enum ColumnType {
    DATA_COLUMN = 0, // normal data column
    ROW_NUMBER = 1,  // row number in a file
};

// Column schema definition shared by table/global projection and file-local schema matching.
//
// ColumnDefinition intentionally carries schema identity only. FE column unique ids are translated
// to GlobalIndex at the FileScannerV2 boundary and must not appear in table/file reader APIs.
struct ColumnDefinition {
    // Identifier used to match a column against another schema.
    //
    // - FIELD_ID: schema evolution aware field id, such as Iceberg/Parquet field id.
    // - NAME: logical column name for formats that rely on names.
    // - POSITION: physical file ordinal for files whose names are placeholders, such as Hive1 ORC.
    struct Identifier {
        enum class Kind {
            INVALID,
            FIELD_ID,
            NAME,
            POSITION,
        };

        Kind kind = Kind::INVALID;
        int32_t field_id = -1;
        std::string name;
        int32_t position = -1;

        static Identifier by_field_id(int32_t id) {
            return {.kind = Kind::FIELD_ID, .field_id = id, .name = {}, .position = -1};
        }

        static Identifier by_name(std::string column_name) {
            return {.kind = Kind::NAME,
                    .field_id = -1,
                    .name = std::move(column_name),
                    .position = -1};
        }

        static Identifier by_position(int32_t file_position) {
            return {.kind = Kind::POSITION, .field_id = -1, .name = {}, .position = file_position};
        }

        bool has_field_id() const { return kind == Kind::FIELD_ID; }
        bool has_name() const { return kind == Kind::NAME; }
        bool has_position() const { return kind == Kind::POSITION; }
    };

    // Matching key from table/global schema to file-local schema.
    //
    // This is not the id that FileReader uses to read data. For example, a Parquet
    // column can be matched by its optional Parquet field_id, while the reader still
    // addresses it by a file-local ordinal.
    Identifier identifier;
    // Reader-local id of this node inside the file schema returned by FileReader::get_schema().
    // Top-level fields use the root column ordinal and nested fields use the child ordinal under
    // their parent. -1 means unset; special virtual file columns may use other negative ids.
    // Table/global ColumnDefinition values can leave this as -1 because they are not read directly
    // by a FileReader.
    int32_t local_id = -1;
    // Logical table column name. This is also the matching name for by-name file formats.
    std::string name;
    DataTypePtr type;
    // Projected nested table children. Children use table/global identifiers; they are resolved to
    // file-local child ids by TableColumnMapper before reaching FileReader.
    std::vector<ColumnDefinition> children {};
    // Expression used to materialize missing/default/generated values when the column is not read
    // directly from the file.
    VExprContextSPtr default_expr = nullptr;
    // Partition columns are constants from split metadata and should not be matched against file
    // schema unless table-format logic explicitly asks for it.
    bool is_partition_key = false;
    // File-local column kind. For table/global columns this remains DATA_COLUMN.
    ColumnType column_type = ColumnType::DATA_COLUMN;

    // Helper for BY_FIELD_ID matching.
    int32_t field_id() const {
        DORIS_CHECK(identifier.has_field_id());
        return identifier.field_id;
    }
    // Helper for BY_INDEX matching.
    int32_t file_position() const {
        DORIS_CHECK(identifier.has_position());
        return identifier.position;
    }
    // Helper for BY_NAME matching.
    const std::string& match_name() const { return identifier.has_name() ? identifier.name : name; }
    // Helper for reader-local projection and scan requests.
    int32_t file_local_id() const {
        if (local_id != -1) {
            return local_id;
        }
        DORIS_CHECK(identifier.has_field_id());
        return identifier.field_id;
    }
};

// Recursive file-local projection path.
//
// For a root entry in FileScanRequest::{predicate_columns, non_predicate_columns}, index is the
// top-level file column id and column_id() is valid. For children, index is the file-local child id
// under the parent node, not a table child id and not a child output ordinal.
//
// project_all_children=true means the whole subtree under this node is needed. When false, children
// lists the selected child paths. File readers can use this to avoid constructing readers for
// unprojected nested children.
struct LocalColumnIndex {
    int32_t index = -1;
    bool project_all_children = true;
    std::vector<LocalColumnIndex> children {};

    static LocalColumnIndex top_level(LocalColumnId column_id) {
        return {.index = column_id.value()};
    }

    static LocalColumnIndex field(int32_t field_id) { return {.index = field_id}; }

    static LocalColumnIndex partial_field(int32_t field_id) {
        return {.index = field_id, .project_all_children = false};
    }

    LocalColumnId column_id() const { return LocalColumnId(index); }
    int32_t field_id() const { return index; }
};

inline bool is_full_projection(const LocalColumnIndex* projection) {
    return projection == nullptr || projection->project_all_children;
}

inline bool is_partial_projection(const LocalColumnIndex* projection) {
    return projection != nullptr && !projection->project_all_children;
}

inline const LocalColumnIndex* find_child_projection(const LocalColumnIndex* projection,
                                                     int32_t field_id) {
    if (is_full_projection(projection)) {
        return nullptr;
    }
    const auto child_it = std::find_if(
            projection->children.begin(), projection->children.end(),
            [&](const LocalColumnIndex& child) { return child.field_id() == field_id; });
    return child_it == projection->children.end() ? nullptr : &*child_it;
}

inline bool is_child_projected(const LocalColumnIndex* projection, int32_t field_id) {
    return is_full_projection(projection) || find_child_projection(projection, field_id) != nullptr;
}

// Merge two projection trees that point to the same file-local node.
//
// A full projection dominates a partial projection. Two partial projections are merged by child id
// and recursively union their child paths. The caller must only merge projections for the same
// root/child node.
inline Status merge_local_column_index(LocalColumnIndex* target, const LocalColumnIndex& source) {
    DORIS_CHECK(target != nullptr);
    DORIS_CHECK(target->index == source.index);
    if (target->project_all_children) {
        return Status::OK();
    }
    if (source.project_all_children) {
        target->project_all_children = true;
        target->children.clear();
        return Status::OK();
    }
    for (const auto& source_child : source.children) {
        auto target_child_it = std::find_if(
                target->children.begin(), target->children.end(),
                [&](const LocalColumnIndex& child) { return child.index == source_child.index; });
        if (target_child_it == target->children.end()) {
            target->children.push_back(source_child);
            continue;
        }
        RETURN_IF_ERROR(merge_local_column_index(&*target_child_it, source_child));
    }
    return Status::OK();
}

} // namespace doris::reader
