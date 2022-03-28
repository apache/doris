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

#include "runtime/tablets_channel.h"

namespace doris {

namespace vectorized {

class VTabletsChannel : public TabletsChannel {

public:
    VTabletsChannel(const TabletsChannelKey& key, bool is_high_priority);

    virtual Status add_block(const PTabletWriterAddBlockRequest& request,
                             PTabletWriterAddBlockResult* response) override;

private:
    virtual Status _open_all_writers(const PTabletWriterOpenRequest& request) override;
};

} // namespace vectorized

} // namespace doris