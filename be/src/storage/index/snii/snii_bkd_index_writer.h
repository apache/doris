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
#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "storage/index/index_writer.h"
#include "storage/index/snii/bkd/bkd_builder.h"
#include "storage/index/snii/bkd/staged_blob_file.h"
#include "storage/index/snii/encoding/byte_sink.h"
#include "storage/olap_common.h"

namespace doris {
class KeyCoder;
class TabletIndex;

namespace segment_v2 {
class IndexFileWriter;

// Doris write-path adapter for the SNII-native BKD (design 10): the numeric
// counterpart of SniiIndexColumnWriter, which serves the text side of the same
// container.
//
// It is NOT a template on FieldType, unlike the CLucene-era
// InvertedIndexColumnWriter<field_type>. Everything that path needed the type
// parameter for -- the value stride and the encoder -- is available at runtime
// from the index's own FieldType (field_type_size + get_key_coder), and
// resolving both from ONE source is exactly what INV-1 asks for: an index
// encoded with a coder other than its recorded field_type's is self-consistent
// but compares in the wrong order, and no round-trip test can see it.
//
// The three sub-files it registers on the container:
//   bkd_data  (cold) -- leaf blocks, sized by the point count, so it is staged
//                       through a temp file rather than held in RAM;
//   bkd_index (hot)  -- the framed header/bounds/splits/leaf directory;
//   bkd_nulls (hot)  -- the SNII null-bitmap POD. NULL rows own no point (a
//                       NULL that leaked in as a point would answer `col > x`),
//                       so they are carried here and nowhere else.
class SniiBkdIndexColumnWriter final : public IndexColumnWriter {
public:
    SniiBkdIndexColumnWriter(IndexFileWriter* index_file_writer, const TabletIndex* index_meta,
                             FieldType value_type);
    ~SniiBkdIndexColumnWriter() override;

    Status init() override;
    Status add_values(const std::string name, const void* values, size_t count) override;
    Status add_array_values(size_t field_size, const void* value_ptr, const uint8_t* null_map,
                            const uint8_t* offsets_ptr, size_t count) override;
    Status add_nulls(uint32_t count) override;
    Status add_array_nulls(const uint8_t* null_map, size_t num_rows) override;
    Status finish() override;
    int64_t size() const override { return 0; }
    void close_on_error() override;

private:
    // Encodes one CppType-wide value at `value` through the index's own key
    // coder and appends it as a point for `docid`.
    Status _add_value(const void* value, uint32_t docid);

    IndexFileWriter* _index_file_writer = nullptr;
    const TabletIndex* _index_meta = nullptr;
    const FieldType _value_type;
    // sizeof(CppType) for _value_type: both the source stride and the point
    // width, which is why they cannot disagree.
    uint32_t _value_size = 0;
    const KeyCoder* _value_key_coder = nullptr;

    // Segment-local row id of the NEXT row to arrive. Advanced by both value
    // runs and null runs, so it is the row count once the column is exhausted.
    uint32_t _rid = 0;
    std::vector<uint32_t> _null_docids;

    std::unique_ptr<::doris::snii::bkd::BkdBuilder> _builder;
    // Staged bkd_data, kept alive until the container has pulled its bytes at
    // IndexFileWriter::finish_close(). Destroying it earlier would unlink the
    // temp file out from under the pull.
    std::shared_ptr<::doris::snii::bkd::StagedBlobFile> _data;
};

} // namespace segment_v2
} // namespace doris
