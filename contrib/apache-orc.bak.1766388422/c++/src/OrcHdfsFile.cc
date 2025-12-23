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

#include "orc/OrcFile.hh"

#include "Adaptor.hh"
#include "Utils.hh"
#include "orc/Exceptions.hh"

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "hdfspp/hdfspp.h"

namespace orc {

  DIAGNOSTIC_PUSH

#ifdef __clang__
  DIAGNOSTIC_IGNORE("-Wunused-private-field")
#endif

  class HdfsFileInputStream : public InputStream {
   private:
    std::string filename;
    std::unique_ptr<hdfs::FileHandle> file;
    std::unique_ptr<hdfs::FileSystem> file_system;
    uint64_t totalLength;
    const uint64_t READ_SIZE = 1024 * 1024;  // 1 MB
    ReaderMetrics* metrics;

   public:
    HdfsFileInputStream(std::string _filename, ReaderMetrics* _metrics) : metrics(_metrics) {
      filename = _filename;

      // Building a URI object from the given uri_path
      hdfs::URI uri;
      try {
        uri = hdfs::URI::parse_from_string(filename);
      } catch (const hdfs::uri_parse_error&) {
        throw ParseError("Malformed URI: " + filename);
      }

      // This sets conf path to default "$HADOOP_CONF_DIR" or "/etc/hadoop/conf"
      // and loads configs core-site.xml and hdfs-site.xml from the conf path
      hdfs::ConfigParser parser;
      if (!parser.LoadDefaultResources()) {
        throw ParseError("Could not load default resources. ");
      }
      auto stats = parser.ValidateResources();
      // validating core-site.xml
      if (!stats[0].second.ok()) {
        throw ParseError(stats[0].first + " is invalid: " + stats[0].second.ToString());
      }
      // validating hdfs-site.xml
      if (!stats[1].second.ok()) {
        throw ParseError(stats[1].first + " is invalid: " + stats[1].second.ToString());
      }
      hdfs::Options options;
      if (!parser.get_options(options)) {
        throw ParseError("Could not load Options object. ");
      }
      hdfs::IoService* io_service = hdfs::IoService::New();
      // Wrapping file_system into a unique pointer to guarantee deletion
      file_system =
          std::unique_ptr<hdfs::FileSystem>(hdfs::FileSystem::New(io_service, "", options));
      if (file_system.get() == nullptr) {
        throw ParseError("Can't create FileSystem object. ");
      }
      hdfs::Status status;
      // Checking if the user supplied the host
      if (!uri.get_host().empty()) {
        // Using port if supplied, otherwise using "" to look up port in configs
        std::string port = uri.has_port() ? std::to_string(uri.get_port()) : "";
        status = file_system->Connect(uri.get_host(), port);
        if (!status.ok()) {
          throw ParseError("Can't connect to " + uri.get_host() + ":" + port + ". " +
                           status.ToString());
        }
      } else {
        status = file_system->ConnectToDefaultFs();
        if (!status.ok()) {
          if (!options.defaultFS.get_host().empty()) {
            throw ParseError("Error connecting to " + options.defaultFS.str() + ". " +
                             status.ToString());
          } else {
            throw ParseError("Error connecting to the cluster: defaultFS is empty. " +
                             status.ToString());
          }
        }
      }

      if (file_system.get() == nullptr) {
        throw ParseError("Can't connect the file system. ");
      }

      hdfs::FileHandle* file_raw = nullptr;
      status = file_system->Open(uri.get_path(), &file_raw);
      if (!status.ok()) {
        throw ParseError("Can't open " + uri.get_path() + ". " + status.ToString());
      }
      // Wrapping file_raw into a unique pointer to guarantee deletion
      file.reset(file_raw);

      hdfs::StatInfo stat_info;
      status = file_system->GetFileInfo(uri.get_path(), stat_info);
      if (!status.ok()) {
        throw ParseError("Can't stat " + uri.get_path() + ". " + status.ToString());
      }
      totalLength = stat_info.length;
    }

    uint64_t getLength() const override {
      return totalLength;
    }

    uint64_t getNaturalReadSize() const override {
      return READ_SIZE;
    }

    void read(void* buf, uint64_t length, uint64_t offset) override {
      SCOPED_STOPWATCH(metrics, IOBlockingLatencyUs, IOCount);
      if (!buf) {
        throw ParseError("Buffer is null");
      }

      char* buf_ptr = reinterpret_cast<char*>(buf);
      hdfs::Status status;
      size_t total_bytes_read = 0;
      size_t last_bytes_read = 0;

      do {
        status =
            file->PositionRead(buf_ptr, static_cast<size_t>(length) - total_bytes_read,
                               static_cast<off_t>(offset + total_bytes_read), &last_bytes_read);
        if (!status.ok()) {
          throw ParseError("Error reading the file: " + status.ToString());
        }
        total_bytes_read += last_bytes_read;
        buf_ptr += last_bytes_read;
      } while (total_bytes_read < length);
    }

    const std::string& getName() const override {
      return filename;
    }

    ~HdfsFileInputStream() override;
  };

  DIAGNOSTIC_POP

  HdfsFileInputStream::~HdfsFileInputStream() {}

  std::unique_ptr<InputStream> readHdfsFile(const std::string& path, ReaderMetrics* metrics) {
    return std::make_unique<HdfsFileInputStream>(path, metrics);
  }
}  // namespace orc
