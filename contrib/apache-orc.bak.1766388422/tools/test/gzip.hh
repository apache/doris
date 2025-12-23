/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef ORC_GZIP
#define ORC_GZIP

#include <stdio.h>
#include <string>
#include "zlib.h"

namespace orc {

  class GzipTextReader {
   private:
    std::string filename;
    FILE* file;
    z_stream stream;
    unsigned char input[64 * 1024];
    unsigned char output[64 * 1024];
    unsigned char* outPtr;
    unsigned char* outEnd;
    bool isDone;

    bool nextBuffer();

    // NOT IMPLEMENTED
    GzipTextReader(const GzipTextReader&);
    GzipTextReader& operator=(const GzipTextReader&);

   public:
    GzipTextReader(const std::string& filename);
    ~GzipTextReader();
    bool nextLine(std::string& line);
  };

}  // namespace orc
#endif
