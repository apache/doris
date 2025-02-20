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

namespace doris {

class FilterBase {
public:
    bool contain_null() const { return _null_aware && _contain_null; }

    void set_contain_null() { _contain_null = true; }

    void set_null_aware(bool null_aware) { _null_aware = null_aware; }

protected:
    bool _contain_null = false;
    bool _null_aware = false;
};

} // namespace doris
