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

#ifndef DORIS_BE_SRC_MEMORY_MEM_TABLET_H_
#define DORIS_BE_SRC_MEMORY_MEM_TABLET_H_

#include "olap/base_tablet.h"

namespace doris {

// Tablet class for memory-optimized storage engine.
//
// It stores all its data in-memory, and is designed for tables with
// frequent updates.
//
// TODO: This is just a skeleton, will add implementation in the future.
class MemTablet : public BaseTablet {

private:
    DISALLOW_COPY_AND_ASSIGN(MemTablet);
};

} /* namespace doris */

#endif /* DORIS_BE_SRC_MEMORY_MEM_TABLET_H_ */
