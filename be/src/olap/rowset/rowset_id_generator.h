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

#ifndef DORIS_BE_SRC_OLAP_ROWSET_ROWSET_ID_GENERATOR_H
#define DORIS_BE_SRC_OLAP_ROWSET_ROWSET_ID_GENERATOR_H

#include "olap/data_dir.h"
#include "olap/olap_define.h"

namespace doris {

class RowsetIdGenerator {

public:    
    ~RowsetIdGenerator() {}

    static RowsetIdGenerator* instance();

    // generator a id according to data dir
    // rowsetid is not globally unique, it is dir level
    // it saves the batch end id into meta env
    OLAPStatus get_next_id(DataDir* dir, RowsetId* rowset_id); 

private:
    RowsetIdGenerator(){}
    RWMutex _ids_lock;
    RowsetId _batch_interval = 10000;
    // data dir -> (cur_id, batch_end_id)
    std::map<DataDir*, std::pair<RowsetId,RowsetId>> _dir_ids; 
    static RowsetIdGenerator* _s_instance;
    static std::mutex _mlock;
}; // RowsetIdGenerator

} // namespace doris
#endif // DORIS_BE_SRC_OLAP_ROWSET_ROWSET_ID_GENERATOR_H
