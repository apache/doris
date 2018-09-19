// Copyright (c) 2018, Baidu.com, Inc. All Rights Reserved

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

#ifndef BDG_PALO_BE_SRC_HTTP_COMPACTION_ACTION_H
#define BDG_PALO_BE_SRC_HTTP_COMPACTION_ACTION_H

#include <boost/scoped_ptr.hpp>

#include "http/http_handler.h"

namespace palo {

class CompactionAction : public HttpHandler {
public:
    void handle(HttpRequest *req) override;
}; // end class ChecksumAction

} // end namespace palo

#endif // BDG_PALO_BE_SRC_HTTP_COMPACTION_ACTION_H
