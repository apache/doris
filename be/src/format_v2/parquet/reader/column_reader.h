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

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "core/column/column_nullable.h"
#include "core/data_type/data_type.h"
#include "format_v2/column_data.h"
#include "format_v2/parquet/parquet_profile.h"
#include "format_v2/parquet/parquet_type.h"
#include "format_v2/parquet/selection_vector.h"
#include "runtime/runtime_profile.h"

namespace parquet {
class ColumnDescriptor;
class RowGroupReader;

namespace internal {
class RecordReader;
} // namespace internal
} // namespace parquet

namespace cctz {
class time_zone;
} // namespace cctz

namespace doris {
class IColumn;
} // namespace doris

namespace doris::format::parquet {
struct ParquetColumnSchema;

// Doris 的 Parquet column reader 抽象基类。
//
// 该类包装 Arrow Parquet RecordReader，负责将 file-local Parquet leaf column 读取成
// Doris-owned column。它不理解 Iceberg/global schema，也不处理 table-level
// cast/default/generated/partition 语义。
//
// 对外提供两组接口：
//
// ① 平铺读取路径（top-level primitive / 复杂类型的整体读取）：
//      read()  — 从当前位置全量读取 rows 行
//      skip()  — row-level 跳过
//      select() — 按 SelectionVector 部分读取（late materialization 的关键）
//
// ② 嵌套读取协议（LIST/MAP/STRUCT 内部的父子协作）：
//      load_nested_batch()     — 加载一批 def/rep levels + values
//      build_nested_column()   — 从 levels 重建嵌套结构并填充值
//      skip_nested_column()    — 跳过一批嵌套数据
//      这个两步协议将 level 解码与值物化分离，让复杂 reader 可以先确定容器结构再按需填充值。
class ParquetColumnReader {
public:
    virtual ~ParquetColumnReader() = default;

    // ========== 标识字段 ==========

    // Reader 在 file_schema 树中的 id。
    // 顶层 reader 返回 root column ordinal，嵌套 reader 返回父节点下的 child ordinal。
    virtual int file_column_id() const { return _field_id; }

    // 该 reader 对应的 Parquet 物理 leaf column id。
    // 用于访问 ColumnDescriptor、RecordReader、ColumnChunk metadata 和 statistics。
    // 例如 MAP<INT, STRING>：顶层 MAP 节点的 parquet_leaf_column_id == file_column_id，
    // 但其子节点 a.key 的 parquet_leaf_column_id == 0（key 列在文件中的物理序号）。
    virtual int parquet_leaf_column_id() const { return _leaf_column_id; }

    // ========== Level 字段 ==========
    // 使本节点自身变为 nullable 的 definition level 阈值。
    // 复杂 reader 用此值区分"我的值是 NULL"和"我有值但内容为空"。
    int16_t nullable_definition_level() const { return _nullable_definition_level; }
    // 最近 repeated 祖先的 repetition level。
    // LIST/MAP reader 用此值从孩子 rep level 流中判断"新元素开始"。
    int16_t repeated_repetition_level() const { return _repeated_repetition_level; }

    virtual const DataTypePtr& type() const { return _type; }
    virtual const std::string& name() const { return _name; }
    const ParquetColumnReaderProfile& profile() const { return _profile; }

    // ========== ① 平铺读取接口 ==========

    // 全量读取：从当前位置读 rows 行，写入 column。
    virtual Status read(int64_t rows, MutableColumnPtr& column, int64_t* rows_read) = 0;

    // 跳过 rows 行。必须使用 row-level skip（推进游标），不能退化为 value-level skip。
    virtual Status skip(int64_t rows);

    // 部分读取：跳过 batch 内未选中的行，只输出 SelectionVector 中标记的行。
    // 用于 late materialization —— predicate 列全量读，non-predicate 列按 selection 读。
    // 该方法只允许 skip + read 推进游标，不允许退化为整批 read + filter。
    virtual Status select(const SelectionVector& sel, uint16_t selected_rows, int64_t batch_rows,
                          MutableColumnPtr& column);

    // ========== ② 嵌套读取协议 ==========
    // 复杂 reader（LIST/MAP/STRUCT）通过这个两步协议与子 reader 协作。

    // 第一步：加载一批嵌套数据。递归调用子 reader，最终到达 leaf reader 的
    // ParquetLeafReader::read_nested_batch()，返回 def/rep levels + values。
    virtual Status load_nested_batch(int64_t rows);

    // 第二步：从已加载的 levels 重建嵌套结构（offsets + null_map）并填充值。
    // length_upper_bound 是预估值，用于提前 reserve 空间。
    virtual Status build_nested_column(int64_t length_upper_bound, MutableColumnPtr& column,
                                       int64_t* values_read);

    // 跳过 rows 行嵌套数据。递归调用子 reader 的 skip_nested_column。
    virtual Status skip_nested_column(int64_t rows);

    // 返回已加载的嵌套 definition/repetition levels（由子 reader 或自身填充）。
    virtual const std::vector<int16_t>& nested_definition_levels() const;
    virtual const std::vector<int16_t>& nested_repetition_levels() const;
    virtual int64_t nested_levels_written() const;
    // 该 reader 自身或其子树中是否包含 repeated 子节点。
    virtual bool is_or_has_repeated_child() const;

    // ========== 嵌套构建游标 ==========
    // 复杂 reader 在 build_nested_column 时分多轮调用子 reader，游标跟踪当前处理到的
    // def/rep level 数组位置，避免重复处理同一批 level。
    int64_t nested_build_level_cursor() const { return _nested_build_level_cursor; }
    void set_nested_build_level_cursor(int64_t cursor) {
        DORIS_CHECK(cursor >= 0);
        _nested_build_level_cursor = cursor;
    }
    void reset_nested_build_level_cursor() { _nested_build_level_cursor = 0; }

protected:
    ParquetColumnReader(const ParquetColumnSchema& schema, const DataTypePtr type,
                        ParquetColumnReaderProfile profile = {});
    ParquetColumnReader() = default;
    void update_reader_read_rows(int64_t rows) const;
    void update_reader_skip_rows(int64_t rows) const;

    ParquetColumnReaderProfile _profile;
    const int _field_id = -1;                     // 在父节点中的 child ordinal
    const int _leaf_column_id = -1;               // Parquet 物理 leaf column id (-1 = 非叶子)
    const int16_t _nullable_definition_level = 0; // 本节点 nullable 的 def level 阈值
    const int16_t _repeated_repetition_level = 0; // 最近 repeated 祖先的 rep level
    const int16_t _definition_level = 0;          // 累计到本节点的 def level
    const int16_t _repetition_level = 0;          // 累计到本节点的 rep level
    const int16_t _repeated_ancestor_definition_level = 0; // 最近 repeated 祖先的 def level
    const DataTypePtr _type;                               // Doris 目标类型
    const std::string _name;                               // 列名（用于报错信息）
    int64_t _nested_build_level_cursor = 0; // 嵌套构建游标（当前处理到的 level 位置）
};

// 为一个 Parquet RowGroup 创建 Doris Column Reader 的工厂。
//
// 工厂持有 RowGroup 级别的共享状态：
// - Arrow RecordReader 实例（按 leaf_column_id 缓存，同一物理列可能被多个 reader 共享）
// - Page skip plans 和 page skip profile（page index 裁剪结果）
// - 标量物化选项：timezone、strict mode 等
//
// 外部调用方只请求顶层列或虚拟扫描列，嵌套子列的递归构造保持私有，
// ParquetScanScheduler 和 ParquetReader 不感知物理 schema 细节。
//
// Projection 支持：当只需要复杂类型的部分子列时（如 MAP 只读 value），
// factory 通过 LocalColumnIndex 参数传递 projection 路径，
// 只为被 projected 的子列创建 reader，跳过不需要的部分。
class ParquetColumnReaderFactory {
public:
    ParquetColumnReaderFactory(std::shared_ptr<::parquet::RowGroupReader> row_group,
                               int num_leaf_columns,
                               const std::map<int, ParquetPageSkipPlan>* page_skip_plans = nullptr,
                               ParquetPageSkipProfile page_skip_profile = {},
                               const cctz::time_zone* timezone = nullptr,
                               bool enable_strict_mode = false,
                               ParquetColumnReaderProfile column_reader_profile = {});

    // 为顶层列 schema 创建 reader。projection 可选，为 nullptr 时读取全部子列，
    // 非 nullptr 时只读取被 projected 的子树部分。
    Status create(const ParquetColumnSchema& column_schema,
                  const format::LocalColumnIndex* projection,
                  std::unique_ptr<ParquetColumnReader>* reader) const;

    // 便捷重载：projection = nullptr（读取全部子列）。
    Status create(const ParquetColumnSchema& column_schema,
                  std::unique_ptr<ParquetColumnReader>* reader) const {
        return create(column_schema, nullptr, reader);
    }

    // 创建虚拟列 reader：生成行位置序号。
    std::unique_ptr<ParquetColumnReader> create_row_position_column_reader(
            int64_t row_group_first_row) const;
    // 创建虚拟列 reader：生成全局唯一 RowId。
    std::unique_ptr<ParquetColumnReader> create_global_rowid_column_reader(
            const format::GlobalRowIdContext& context, int64_t row_group_first_row) const;

private:
    // 创建基本类型叶子的 reader。
    // is_nested=true 表示该叶子在复杂类型内部，允许携带 def/rep levels。
    // is_nested=false 表示顶层平铺列，需要做额外的 flat layout 校验。
    Status create_scalar_column_reader(const ParquetColumnSchema& column_schema, bool is_nested,
                                       std::unique_ptr<ParquetColumnReader>* reader) const;

    // 创建 STRUCT reader。递归为 projected 子列创建 reader。
    // 部分 projection 时重建 DataTypeStruct，使物化结果只包含被 projected 的子字段。
    Status create_struct_column_reader(const ParquetColumnSchema& column_schema,
                                       const format::LocalColumnIndex* projection,
                                       std::unique_ptr<ParquetColumnReader>* reader) const;

    // 创建 LIST reader，持有单个 element reader。
    // element 被部分 projection 时，重建 DataTypeArray 使用 projected 的 element type。
    Status create_list_column_reader(const ParquetColumnSchema& column_schema,
                                     const format::LocalColumnIndex* projection,
                                     std::unique_ptr<ParquetColumnReader>* reader) const;

    // 创建 MAP reader，持有 key reader + value reader。
    // Schema 构建时已折叠 key_value/entry wrapper，children 直接是 [key, value]。
    // 部分 MAP projection 仅对 value 子树做裁剪。key 流始终完整读取，
    // 因为它拥有 entry 的存在性、offsets 和 key equality 语义。
    Status create_map_column_reader(const ParquetColumnSchema& column_schema,
                                    const format::LocalColumnIndex* projection,
                                    std::unique_ptr<ParquetColumnReader>* reader) const;

    // 私有递归分发器。根据 ParquetColumnSchema::kind 路由到对应的 create_* 方法。
    // is_nested 为 true 表示该节点属于复杂 reader 的子节点，控制 primitive leaf 的校验逻辑；
    // 复杂 reader 总是从规范化的 ParquetColumnSchema 子树创建。
    Status create_column_reader(const ParquetColumnSchema& column_schema,
                                const format::LocalColumnIndex* projection, bool is_nested,
                                std::unique_ptr<ParquetColumnReader>* reader) const;

    // 惰性创建并缓存 Arrow RecordReader（按 leaf_column_id 索引）。
    // 多个 Doris reader 可能通过不同嵌套路径共享同一个物理列的数据流，
    // 因此 RecordReader 的生命周期绑定到 RowGroup 工厂。
    Status get_record_reader(int leaf_column_id, const ::parquet::ColumnDescriptor* descriptor,
                             const std::string& name,
                             std::shared_ptr<::parquet::internal::RecordReader>* reader) const;

    // 在 schema 校验和 RecordReader 查找完成后，最终构造 ScalarColumnReader。
    Status make_scalar_column_reader(
            const ParquetColumnSchema& column_schema,
            std::shared_ptr<::parquet::internal::RecordReader> record_reader,
            std::unique_ptr<ParquetColumnReader>* reader) const;

    std::shared_ptr<::parquet::RowGroupReader> _row_group; // Arrow RowGroup 读取器
    mutable std::vector<std::shared_ptr<::parquet::internal::RecordReader>>
            _record_readers; // RecordReader 缓存(按 leaf_column_id)
    const std::map<int, ParquetPageSkipPlan>* _page_skip_plans = nullptr; // page index 裁剪结果
    ParquetPageSkipProfile _page_skip_profile;                            // page skip profile
    const cctz::time_zone* _timezone = nullptr;                           // 时区
    bool _enable_strict_mode = false;                                     // 严格模式
    ParquetColumnReaderProfile _column_reader_profile;                    // column reader profile
};
} // namespace doris::format::parquet
