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

#include "gzip.hh"
#include "Adaptor.hh"

#include <iostream>
#include <stdexcept>

#ifdef __clang__
#pragma clang diagnostic ignored "-Wold-style-cast"
#endif

namespace orc {

  GzipTextReader::GzipTextReader(const std::string& _filename) : filename(_filename) {
    file = fopen(filename.c_str(), "rb");
    if (file == nullptr) {
      throw std::runtime_error("can't open " + filename);
    }
    stream.zalloc = nullptr;
    stream.zfree = nullptr;
    stream.opaque = nullptr;
    stream.avail_in = 0;
    stream.avail_out = 1;
    stream.next_in = nullptr;
    int ret = inflateInit2(&stream, 16 + MAX_WBITS);
    if (ret != Z_OK) {
      throw std::runtime_error("zlib failed initialization for " + filename);
    }
    outPtr = nullptr;
    outEnd = nullptr;
    isDone = false;
  }

  bool GzipTextReader::nextBuffer() {
    // if we are done, return
    if (isDone) {
      return false;
    }
    // if the last read is done, read more
    if (stream.avail_in == 0 && stream.avail_out != 0) {
      stream.next_in = input;
      stream.avail_in = static_cast<unsigned>(fread(input, 1, sizeof(input), file));
      if (ferror(file)) {
        throw std::runtime_error("failure reading " + filename);
      }
    }
    stream.avail_out = sizeof(output);
    stream.next_out = output;
    int ret = inflate(&stream, Z_NO_FLUSH);
    switch (ret) {
      case Z_OK:
        break;
      case Z_STREAM_END:
        isDone = true;
        break;
      case Z_STREAM_ERROR:
        throw std::runtime_error("zlib stream problem");
      case Z_NEED_DICT:
      case Z_DATA_ERROR:
        throw std::runtime_error("zlib data problem");
      case Z_MEM_ERROR:
        throw std::runtime_error("zlib memory problem");
      case Z_BUF_ERROR:
        throw std::runtime_error("zlib buffer problem");
      default:
        throw std::runtime_error("zlib unknown problem");
    }
    outPtr = output;
    outEnd = output + (sizeof(output) - stream.avail_out);
    return true;
  }

  bool GzipTextReader::nextLine(std::string& line) {
    bool result = false;
    line.clear();
    while (true) {
      if (outPtr == outEnd) {
        if (!nextBuffer()) {
          return result;
        }
      }
      unsigned char ch = *(outPtr++);
      if (ch == '\n') {
        return true;
      }
      line += static_cast<char>(ch);
    }
  }

  GzipTextReader::~GzipTextReader() {
    inflateEnd(&stream);
    if (fclose(file) != 0) {
      std::cerr << "can't close file " << filename;
    }
  }
}  // namespace orc
