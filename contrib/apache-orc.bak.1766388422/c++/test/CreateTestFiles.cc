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

#include "Adaptor.hh"
#include "wrap/orc-proto-wrapper.hh"

#include <fstream>
#include <iostream>
#include <string>

/**
 * Write an empty custom ORC file with a lot of control over format.
 * We use this to create files to test the reader rather than anything
 * that users would want to do.
 */
void writeCustomOrcFile(const std::string& filename, const orc::proto::Metadata& metadata,
                        const orc::proto::Footer& footer, const std::vector<std::uint32_t>& version,
                        std::uint32_t writerVersion) {
  std::fstream output(filename.c_str(), std::ios::out | std::ios::trunc | std::ios::binary);
  output << "ORC";
  if (!metadata.SerializeToOstream(&output)) {
    std::cerr << "Failed to write metadata for " << filename << "\n";
    exit(1);
  }
  if (!footer.SerializeToOstream(&output)) {
    std::cerr << "Failed to write footer for " << filename << "\n";
    exit(1);
  }
  orc::proto::PostScript ps;
  ps.set_footerlength(static_cast<uint64_t>(footer.ByteSizeLong()));
  ps.set_compression(orc::proto::NONE);
  ps.set_compressionblocksize(64 * 1024);
  for (size_t i = 0; i < version.size(); ++i) {
    ps.add_version(version[i]);
  }
  ps.set_metadatalength(static_cast<uint64_t>(metadata.ByteSizeLong()));
  ps.set_writerversion(writerVersion);
  ps.set_magic("ORC");
  if (!ps.SerializeToOstream(&output)) {
    std::cerr << "Failed to write postscript for " << filename << "\n";
    exit(1);
  }
  output.put(static_cast<char>(ps.ByteSizeLong()));
}

/**
 * Create a file from a future version 19.99.
 */
void writeVersion1999() {
  orc::proto::Metadata meta;
  orc::proto::Footer footer;
  footer.set_headerlength(3);
  footer.set_contentlength(3);
  orc::proto::Type* type = footer.add_types();
  type->set_kind(orc::proto::Type_Kind_STRUCT);
  footer.set_numberofrows(0);
  footer.set_rowindexstride(10000);
  orc::proto::ColumnStatistics* stats = footer.add_statistics();
  stats->set_numberofvalues(0);
  stats->set_hasnull(false);
  std::vector<std::uint32_t> version;
  version.push_back(19);
  version.push_back(99);
  writeCustomOrcFile("version1999.orc", meta, footer, version, 1);
}

int main(int, char*[]) {
  writeVersion1999();
  google::protobuf::ShutdownProtobufLibrary();
  return 0;
}
