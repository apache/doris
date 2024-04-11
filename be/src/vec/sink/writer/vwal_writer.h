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
#include <string>
#include <vector>

#include "common/status.h"
#include "olap/wal/wal_manager.h"
#include "olap/wal/wal_writer.h"
#include "vec/core/block.h"

namespace doris {
namespace vectorized {

class VWalWriter {
public:
    VWalWriter(int64_t db_id, int64_t tb_id, int64_t wal_id, const std::string& import_label,
               WalManager* wal_manager, std::vector<TSlotDescriptor>& slot_desc,
               int be_exe_version);
    ~VWalWriter();
    Status init();
    Status write_wal(vectorized::Block* block);
    Status close();

private:
    Status _create_wal_writer(int64_t wal_id, std::shared_ptr<WalWriter>& wal_writer);

private:
    int64_t _db_id;
    int64_t _tb_id;
    int64_t _wal_id;
    std::string _label;
    WalManager* _wal_manager;
    std::vector<TSlotDescriptor>& _slot_descs;
    int _be_exe_version = 0;
    std::shared_ptr<WalWriter> _wal_writer;
};
} // namespace vectorized
} // namespace doris