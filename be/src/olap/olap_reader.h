// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#ifndef BDG_PALO_BE_SRC_OLAP_OLAP_READER_H
#define BDG_PALO_BE_SRC_OLAP_OLAP_READER_H

#include <gen_cpp/PaloInternalService_types.h>
#include <thrift/protocol/TDebugProtocol.h>

#include "common/object_pool.h"
#include "common/status.h"
#include "olap/delete_handler.h"
#include "olap/i_data.h"
#include "olap/olap_cond.h"
#include "olap/olap_engine.h"
#include "olap/reader.h"

namespace palo {

class OLAPShowHints {
public:
    static Status show_hints(
            TShowHintsRequest& fetch_request,
            std::vector<std::vector<std::vector<std::string>>>* ranges,
            RuntimeProfile* profile);
};

}  // namespace palo

#endif // BDG_PALO_BE_SRC_OLAP_OLAP_READER_H
