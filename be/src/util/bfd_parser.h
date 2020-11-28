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

#ifndef DORIS_BE_SRC_UTIL_BFD_PARSER_H
#define DORIS_BE_SRC_UTIL_BFD_PARSER_H

#ifndef PACKAGE
#define PACKAGE
#endif

#ifndef PACKAGE_VERSION
#define PACKAGE_VERSION
#endif

#include <bfd.h>

#include <mutex>
#include <string>
#include <vector>

namespace doris {

class BfdParser {
public:
    // Create parser for running process
    static BfdParser* create();
    static BfdParser* create(const std::string& file_name);

    BfdParser(const std::string& file_name);
    ~BfdParser();
    int parse();

    // Decode address to function_name file_name and line number
    // Call parse before call this function
    // Return 0 if found and fill file_name, function_name, lineno
    //  -1 otherwise
    int decode_address(const char* str, const char** end, std::string* file_name,
                       std::string* function_name, unsigned int* lineno);

    long num_symbols() const { return _num_symbols; }

    static void list_targets(std::vector<std::string>* targets);
    void list_sections(std::string* ss);

private:
    static void init_bfd();

    int open_bfd();
    int load_symbols();

    static std::mutex _bfd_mutex;
    static bool _is_bfd_inited;

    std::string _file_name;
    std::mutex _mutex;
    bfd* _abfd;
    bfd_symbol** _syms;
    long _num_symbols;
    unsigned int _symbol_size;
};

} // namespace doris

#endif
