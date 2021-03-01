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

#include "runtime/tuple_row.h"

#include <sstream>

namespace doris {

std::string TupleRow::to_string(const RowDescriptor& d) {
    std::stringstream out;
    out << "[";
    for (int i = 0; i < d.tuple_descriptors().size(); ++i) {
        if (i != 0) {
            out << " ";
        }
        out << Tuple::to_string(get_tuple(i), *d.tuple_descriptors()[i]);
    }

    out << "]";
    return out.str();
}

} // namespace doris
