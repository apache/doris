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

#include "olap/rowset/segment_v2/vector_index_writer.h"
#include "olap/rowset/segment_v2/vector_index_desc.h"

#include "mindann/mindann_index_utils.h"

namespace doris::segment_v2 {

Status VectorIndexWriter::init(const TabletIndex* tablet_index) {
    const std::map<string, string>& index_params = tablet_index->properties();

    // init index_type
    auto it = index_params.find(INDEX_TYPE);
    if (it != index_params.end()) {
        string standard_type_string = boost::algorithm::to_lower_copy(it->second);
        if (standard_type_string == "sparse_wand" || standard_type_string == "sparse_inverted_index") {
            _index_family = 1;
        }
    }

    if (_index_family == 0) {
        _dense_index_builder = std::make_unique<MindAnnIndexBuilder>(_segment_index_path, _src_is_nullable);
        RETURN_IF_ERROR(_dense_index_builder->init(tablet_index));
    } else if (_index_family == 1) {
        // index_type=sparse_wand, use_wand=true;
        // index_type=sparse_inverted_index, use_wand=false
        _sparse_index_builder = std::make_unique<sparse::SparseInvertedIndexBuilder>(_fs, _segment_index_path);
        RETURN_IF_ERROR(_sparse_index_builder->init());
    }

    return Status::OK();
}

Status VectorIndexWriter::add_array_float_values(const uint8_t* raw_data_ptr,
                                                 const size_t count,
                                                 const uint8_t* offsets_ptr) {
    return _dense_index_builder->add_array_float_values(raw_data_ptr, count, offsets_ptr);
}

Status VectorIndexWriter::add_map_int_to_float_values(const uint8_t* raw_key_data_ptr,
                                                      const uint8_t* raw_value_data_ptr,
                                                      const size_t count,
                                                      const uint8_t* offsets_ptr) {
    return _sparse_index_builder->add_map_int_to_float_values(raw_key_data_ptr,
                                                              raw_value_data_ptr,
                                                              count,
                                                              offsets_ptr);
}

Status VectorIndexWriter::finish() {
    if (_index_family == 0) {
        // For ivfpq index, we flush index file when row_count >= build_threshold,
        // But the null value needs special treatment: We send null values to builder first,
        // builder will filter null values and add remaining values to mindann,
        // we will decide whether to flush index according to the count values truly added.
        if ( _dense_index_builder->get_added_count() >= _dense_index_builder->get_vector_index_build_threshold()) {
            RETURN_IF_ERROR(_dense_index_builder->flush());
            _dense_index_builder->close();
        } else {
            // flush with empty mark
            RETURN_IF_ERROR(flush_empty(_segment_index_path));
            _dense_index_builder->abort();
        }
    } else if (_index_family == 1) {
        RETURN_IF_ERROR(_sparse_index_builder->flush());
        RETURN_IF_ERROR(_sparse_index_builder->close());
    }

    return Status::OK();
}

Status VectorIndexWriter::flush_empty(const std::string& segment_index_path) {
    RETURN_IF_ERROR(_fs->create_file(segment_index_path, &_file_writer));
    RETURN_IF_ERROR(_file_writer->append(VectorIndexDescriptor::mark_word));
    RETURN_IF_ERROR(_file_writer->finalize());
    RETURN_IF_ERROR(_file_writer->close());
    _file_writer.reset();
    return Status::OK();
}

}