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

#include <cstddef>
#include <cstdint>
#include <map>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/factory_creator.h"
#include "format/generic_reader.h"
#include "io/file_factory.h"
#include "runtime/runtime_profile.h"

namespace doris {
class RuntimeProfile;
class RuntimeState;
class SlotDescriptor;

class FilesetReader : public GenericReader {
    ENABLE_FACTORY_CREATOR(FilesetReader);

public:
    FilesetReader(const std::vector<SlotDescriptor*>& file_slot_descs, RuntimeState* state,
                  RuntimeProfile* profile, const std::map<std::string, std::string>& fileset_params);
    ~FilesetReader() override = default;

    Status init_reader();
    Status get_next_block(Block* block, size_t* read_rows, bool* eof) override;
    Status get_columns(std::unordered_map<std::string, DataTypePtr>* name_to_type,
                       std::unordered_set<std::string>* missing_cols) override;

private:
    struct FilesetProfile {
        RuntimeProfile::Counter* listed_files = nullptr;
        RuntimeProfile::Counter* listed_bytes = nullptr;
        RuntimeProfile::Counter* emitted_rows = nullptr;
    };

    Status _build_files();
    Status _list_files(const io::FileSystemSPtr& fs, const std::string& table_path);
    static Result<TFileType::type> _parse_file_type(const std::string& file_type);
    static std::string _build_object_uri(const std::string& table_path, const std::string& listed_name);
    void _init_profile();
    void _write_file_jsonb(JsonbWriter& writer, const io::FileInfo& file);

    const std::vector<SlotDescriptor*>& _file_slot_descs;
    RuntimeState* _state;
    RuntimeProfile* _profile;
    FilesetProfile _fileset_profile;
    std::map<std::string, std::string> _fileset_params;
    std::vector<io::FileInfo> _files;
    size_t _next_file_idx = 0;
};
} // namespace doris
