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

#include <memory>
#include <string>

#include "format_v2/parquet/parquet_type.h"
#include "format_v2/parquet/reader/column_reader.h"
#include "format_v2/parquet/reader/parquet_leaf_reader.h"

namespace parquet {
class ColumnDescriptor;

namespace internal {
class RecordReader;
} // namespace internal
} // namespace parquet

namespace cctz {
class time_zone;
} // namespace cctz

namespace doris::format::parquet {

// 基本类型列的读取器，直接持有 Arrow RecordReader 并通过 ParquetLeafReader 读写值。
//
// 这是所有 ColumnReader 中唯一直接与 Arrow RecordReader 交互的 reader。
// 它同时服务于两种场景：
//   1. 顶层平铺列（如 SELECT id, name FROM t）→ 通过 read()/skip()/select()
//   2. 复杂类型内部的叶子（如 MAP 的 key/value、LIST 的 element）→ 通过嵌套协议
//      load_nested_batch() / build_nested_column()
//
// MapColumnReader 被声明为 friend，因为它需要直接访问 descriptor() 和 leaf_reader()
// 来读取 key 列的 values 用于 entry 存在性校验。
class ScalarColumnReader final : public ParquetColumnReader {
    friend class MapColumnReader;

public:
    ScalarColumnReader(const ParquetColumnSchema& column_schema,
                       std::shared_ptr<::parquet::internal::RecordReader> record_reader,
                       const ParquetPageSkipPlan* page_skip_plan = nullptr,
                       const cctz::time_zone* timezone = nullptr, bool enable_strict_mode = false,
                       ParquetColumnReaderProfile profile = {});
    ~ScalarColumnReader() override;

    // ========== ① 平铺读取 ==========

    Status read(int64_t rows, MutableColumnPtr& column, int64_t* rows_read) override;
    Status skip(int64_t rows) override;

    // ========== ② 嵌套协议 ==========

    Status load_nested_batch(int64_t rows) override;
    Status build_nested_column(int64_t length_upper_bound, MutableColumnPtr& column,
                               int64_t* values_read) override;
    const std::vector<int16_t>& nested_definition_levels() const override;
    const std::vector<int16_t>& nested_repetition_levels() const override;
    int64_t nested_levels_written() const override;
    bool is_or_has_repeated_child() const override;

private:
    // 将嵌套 batch 中单个 level_idx 对应的值写入目标 column。
    // 被 MapColumnReader 用来逐个填充 value。
    Status append_nested_value(int64_t level_idx, MutableColumnPtr& column) const;

    const ::parquet::ColumnDescriptor* descriptor() const { return _descriptor; }

    // 创建临时 ParquetLeafReader。每次调用都创建新实例（轻量级，只持有 shared_ptr）。
    ParquetLeafReader leaf_reader() const {
        return ParquetLeafReader(_descriptor, _type_descriptor, _type, _name, _record_reader,
                                 _profile, _timezone, _enable_strict_mode);
    }

    void advance_rows_read(int64_t rows);
    Status skip_records(int64_t rows);
    // 计算 page skip 优化下当前 batch 内可以通过 page skip 跳过的行数。
    int64_t page_filtered_rows_to_skip(int64_t rows) const;

    const ::parquet::ColumnDescriptor* _descriptor = nullptr;          // Arrow 列描述符
    ParquetTypeDescriptor _type_descriptor;                            // 类型编码信息
    std::shared_ptr<::parquet::internal::RecordReader> _record_reader; // Arrow 物理列读取器
    const ParquetPageSkipPlan* _page_skip_plan = nullptr; // page index 裁剪结果（可为 nullptr）
    const cctz::time_zone* _timezone = nullptr;           // 时区
    bool _enable_strict_mode = false;                     // 严格模式
    int64_t _row_group_rows_read = 0;                     // 当前 RG 已读行数（游标）
    std::unique_ptr<ParquetNestedScalarBatch> _nested_batch; // 嵌套读取的中间结果
};

} // namespace doris::format::parquet
