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

#ifndef BDG_PALO_BE_SRC_EXEC_FILE_WRITER_H
#define BDG_PALO_BE_SRC_EXEC_FILE_WRITER_H

#include <stdint.h>

#include "common/status.h"

namespace palo {

class FileWriter {
public:
    virtual ~FileWriter() {
    }

    virtual Status open() = 0;

    // Writes up to count bytes from the buffer pointed buf to the file.
    // NOTE: the number of bytes written may be less than count if.
    virtual Status write(const uint8_t* buf, size_t buf_len, size_t* written_len) = 0;

    virtual void close() = 0;
};

} // end namespace palo

#endif // BDG_PALO_BE_SRC_EXEC_FILE_WRITER_H
