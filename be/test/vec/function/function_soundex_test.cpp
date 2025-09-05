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

#include <string>
#include <vector>

#include "function_test_util.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_string.h"

namespace doris::vectorized {

TEST(SoundexFunctionTest, soundex_basic_test) {
    std::string func_name = "soundex";

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR};

        DataSet data_set = {
                {{std::string("Doris")}, std::string("D620")},
                {{std::string("Robert")}, std::string("R163")},
                {{std::string("Rupert")}, std::string("R163")},
                {{std::string("Smith")}, std::string("S530")},
                {{std::string("Smyth")}, std::string("S530")},
                {{std::string("Johnson")}, std::string("J525")},
                {{std::string("Jackson")}, std::string("J250")},
                {{std::string("Ashcraft")}, std::string("A261")},
                {{std::string("Ashcroft")}, std::string("A261")},
                {{std::string("Washington")}, std::string("W252")},
                {{std::string("Lee")}, std::string("L000")},
                {{std::string("Gutierrez")}, std::string("G362")},
                {{std::string("Pfister")}, std::string("P236")},
                {{std::string("Honeyman")}, std::string("H555")},
                {{std::string("Lloyd")}, std::string("L300")},
                {{std::string("Tymczak")}, std::string("T522")},

                {{std::string("A")}, std::string("A000")},
                {{std::string("B")}, std::string("B000")},
                {{std::string("Z")}, std::string("Z000")},

                {{std::string("robert")}, std::string("R163")},
                {{std::string("ROBERT")}, std::string("R163")},
                {{std::string("RoBerT")}, std::string("R163")},

                {{std::string("R@bert")}, std::string("R163")},
                {{std::string("Rob3rt")}, std::string("R163")},
                {{std::string("Rob-ert")}, std::string("R163")},
                {{std::string("123Robert")}, std::string("R163")},
                {{std::string("123")}, std::string("")},
                {{std::string("@#$")}, std::string("")},
                {{std::string("   ")}, std::string("")},
                {{std::string("")}, std::string("")},
                {{std::string("Ab_+ %*^cdefghijklmnopqrstuvwxyz")}, std::string("A123")},

                {{std::string("Euler")}, std::string("E460")},
                {{std::string("Gauss")}, std::string("G200")},
                {{std::string("Hilbert")}, std::string("H416")},
                {{std::string("Knuth")}, std::string("K530")},
                {{std::string("Lloyd")}, std::string("L300")},
                {{std::string("Lukasiewicz")}, std::string("L222")},

                {{std::string("Huang")}, std::string("H520")},
                {{std::string("Zhang")}, std::string("Z520")},
                {{std::string("Wang")}, std::string("W520")}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }
}

} // namespace doris::vectorized