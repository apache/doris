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
#include<string>
#include<vector>

namespace doris {

class ESScrollQueryBuilder {

public:
    ESScrollQueryBuilder();
    ~ESScrollQueryBuilder();
    // build the query DSL for elasticsearch
    std::string build();
    
    
    void set_batch_size(uint16_t batch_size) {
        _size = batch_size;
    }
    void set_selected_fields(std::vector<std::string>& fields) {
        _fields = fields;
    }

private:
    std::vector<std::string> _fields;
    uint16_t _size;
};
}
