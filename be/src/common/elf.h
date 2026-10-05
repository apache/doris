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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/Elf.h
// and modified by Doris

#pragma once

#if defined(__ELF__) && !defined(__FreeBSD__)

#include <elf.h>
#include <link.h>

#include <filesystem>
#include <functional>
#include <optional>
#include <string>

using ElfAddr = ElfW(Addr);
using ElfEhdr = ElfW(Ehdr);
using ElfOff = ElfW(Off);
using ElfPhdr = ElfW(Phdr);
using ElfShdr = ElfW(Shdr);
using ElfNhdr = ElfW(Nhdr);
using ElfSym = ElfW(Sym);

namespace doris {

/** Allow to navigate sections in ELF.
  */
class Elf final {
public:
    struct Section {
        const ElfShdr& header;
        [[nodiscard]] const char* name() const;

        [[nodiscard]] const char* begin() const;
        [[nodiscard]] const char* end() const;
        [[nodiscard]] size_t size() const;

        Section(const ElfShdr& header_, const Elf& elf_);

    private:
        const Elf& elf;
    };

    explicit Elf(const std::string& path);
    ~Elf();

    bool iterateSections(std::function<bool(const Section& section, size_t idx)>&& pred) const;
    std::optional<Section> findSection(
            std::function<bool(const Section& section, size_t idx)>&& pred) const;
    std::optional<Section> findSectionByName(const char* name) const;

    [[nodiscard]] const char* begin() const { return mapped; }
    [[nodiscard]] const char* end() const { return mapped + elf_size; }
    [[nodiscard]] size_t size() const { return elf_size; }

    /// Obtain build id from SHT_NOTE of section headers (fallback to PT_NOTES section of program headers).
    /// Return empty string if does not exist.
    /// The string is returned in binary. Note that "readelf -n ./clickhouse-server" prints it in hex.
    [[nodiscard]] std::string getBuildID() const;
    static std::string getBuildID(const char* nhdr_pos, size_t size);

    /// Hash of the binary for integrity checks.
    [[nodiscard]] std::string getStoredBinaryHash() const;

private:
    int _fd = -1;
    std::filesystem::path _file;
    size_t elf_size;
    char* mapped = nullptr;
    const ElfEhdr* header = nullptr;
    const ElfShdr* section_headers = nullptr;
    const ElfPhdr* program_headers = nullptr;
    const char* section_names = nullptr;
};

} // namespace doris

#endif
