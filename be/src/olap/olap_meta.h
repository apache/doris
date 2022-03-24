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

#ifndef DORIS_BE_SRC_OLAP_OLAP_OLAP_META_H
#define DORIS_BE_SRC_OLAP_OLAP_OLAP_META_H

#include <functional>
#include <map>
#include <string>

#include "olap/olap_define.h"
#include "rocksdb/db.h"

namespace doris {

class OlapMeta {
public:
    OlapMeta(const std::string& root_path);

    virtual ~OlapMeta();

    OLAPStatus init();

    OLAPStatus get(const int column_family_index, const std::string& key, std::string* value);

    bool key_may_exist(const int column_family_index, const std::string& key, std::string* value);

    OLAPStatus put(const int column_family_index, const std::string& key, const std::string& value);

    OLAPStatus remove(const int column_family_index, const std::string& key);

    OLAPStatus iterate(const int column_family_index, const std::string& prefix,
                       std::function<bool(const std::string&, const std::string&)> const& func);

    std::string get_root_path();

private:
    std::string _root_path;
    rocksdb::DB* _db;
    std::vector<rocksdb::ColumnFamilyHandle*> _handles;
};

} // namespace doris

#endif // DORIS_BE_SRC_OLAP_OLAP_OLAP_META_H
