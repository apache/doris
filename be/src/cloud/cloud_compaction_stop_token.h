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

#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_tablet.h"

namespace doris {

class CloudCompactionStopToken {
public:
    CloudCompactionStopToken(CloudStorageEngine& engine, CloudTabletSPtr tablet, int64_t initiator);
    ~CloudCompactionStopToken() = default;

    void do_lease();
    Status do_register();
    Status do_unregister();

    int64_t initiator() const;

private:
    CloudStorageEngine& _engine;
    CloudTabletSPtr _tablet;
    std::string _uuid;
    int64_t _initiator;
};

} // namespace doris
