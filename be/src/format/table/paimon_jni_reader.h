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
#include <map>
#include <string>
#include <vector>

#include "common/status.h"
#include "format/jni/jni_reader.h"

namespace doris {
class RuntimeProfile;
class RuntimeState;
class SlotDescriptor;
class Block;
} // namespace doris

namespace doris {
/**
 * The demo usage of JniReader, showing how to read data from java scanner.
 * The java side is also a mock reader that provide values for each type.
 * This class will only be retained during the functional testing phase to verify that
 * the communication and data exchange with the jvm are correct.
 */
class PaimonJniReader : public JniReader {
    ENABLE_FACTORY_CREATOR(PaimonJniReader);

public:
    static const std::string PAIMON_OPTION_PREFIX;
    static const std::string HADOOP_OPTION_PREFIX;
    static const std::string DORIS_ENABLE_JNI_IO_MANAGER;
    static const std::string DORIS_JNI_IO_MANAGER_TMP_DIR;
    PaimonJniReader(const std::vector<SlotDescriptor*>& file_slot_descs, RuntimeState* state,
                    RuntimeProfile* profile, const TFileRangeDesc& range,
                    const TFileScanRangeParams* range_params);

    ~PaimonJniReader() override = default;

    Status _do_get_next_block(Block* block, size_t* read_rows, bool* eof) override;

    Status init_reader();
    Status prepare_split(const TFileRangeDesc& range, ReaderInitContext* ctx);
    Status finish_split();
    bool can_reuse_for(const TFileRangeDesc& range) const;

protected:
    Status _do_init_reader(ReaderInitContext* /*ctx*/) override { return init_reader(); }

private:
    Status prepare_jni_scanner_for_split(const TFileRangeDesc& range);
    Status reset_jni_scanner_current_split();
    static std::map<std::string, std::string> build_scanner_params(
            const std::vector<SlotDescriptor*>& file_slot_descs, RuntimeState* state,
            const TFileRangeDesc& range, const TFileScanRangeParams* range_params);
    void set_remaining_table_level_row_count(const TFileRangeDesc& range);

    const TFileScanRangeParams* _range_params = nullptr;
    std::map<std::string, std::string> _scanner_params_signature;
    int64_t _remaining_table_level_row_count;
    bool _current_split_prepared = false;
};

} // namespace doris
