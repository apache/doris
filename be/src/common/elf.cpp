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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/Elf.cpp
// and modified by Doris

#if defined(__ELF__) && !defined(__FreeBSD__)

#include <common/elf.h>
#include <fcntl.h>
#include <fmt/format.h>
#include <sys/mman.h>
#include <unistd.h>
#include <vec/common/unaligned.h>

#include <cstring>
#include <system_error>

#include "common/logging.h"
#include "common/macros.h"

namespace doris {

Elf::Elf(const std::string& path) {
    _file = path;
    std::error_code ec;
    elf_size = std::filesystem::file_size(_file, ec);
    if (ec) {
        LOG(FATAL) << fmt::format("failed to get file size {}: ({}), {}", _file.native(),
                                  ec.value(), ec.message());
    }
    /// Check if it's an elf.
    if (elf_size < sizeof(ElfEhdr)) {
        LOG(FATAL) << fmt::format("The size of supposedly ELF file '{}' is too small", path);
    }
    RETRY_ON_EINTR(_fd, open(_file.c_str(), O_RDONLY));
    if (_fd < 0) {
        LOG(FATAL) << fmt::format("failed to open {}", _file.native());
    }
    mapped = static_cast<char*>(mmap(nullptr, elf_size, PROT_READ, MAP_SHARED, _fd, 0));
    if (MAP_FAILED == mapped) {
        LOG(FATAL) << fmt::format("MMappedFileDescriptor: Cannot mmap {}, read from {}.", elf_size,
                                  path);
    }

    header = reinterpret_cast<const ElfEhdr*>(mapped);

    if (memcmp(header->e_ident,
               "\x7F"
               "ELF",
               4) != 0) {
        LOG(FATAL) << fmt::format("The file '{}' is not ELF according to magic", path);
    }

    /// Get section header.
    ElfOff section_header_offset = header->e_shoff;
    uint16_t section_header_num_entries = header->e_shnum;

    if (!section_header_offset || !section_header_num_entries ||
        section_header_offset + section_header_num_entries * sizeof(ElfShdr) > elf_size) {
        LOG(FATAL) << fmt::format(
                "The ELF '{}' is truncated (section header points after end of file)", path);
    }

    section_headers = reinterpret_cast<const ElfShdr*>(mapped + section_header_offset);

    /// The string table with section names.
    auto section_names_strtab = findSection([&](const Section& section, size_t idx) {
        return section.header.sh_type == SHT_STRTAB && header->e_shstrndx == idx;
    });

    if (!section_names_strtab) {
        LOG(FATAL) << fmt::format("The ELF '{}' doesn't have string table with section names",
                                  path);
    }

    ElfOff section_names_offset = section_names_strtab->header.sh_offset;
    if (section_names_offset >= elf_size) {
        LOG(FATAL) << fmt::format(
                "The ELF '{}' is truncated (section names string table points after end of file)",
                path);
    }
    section_names = reinterpret_cast<const char*>(mapped + section_names_offset);

    /// Get program headers

    ElfOff program_header_offset = header->e_phoff;
    uint16_t program_header_num_entries = header->e_phnum;

    if (!program_header_offset || !program_header_num_entries ||
        program_header_offset + program_header_num_entries * sizeof(ElfPhdr) > elf_size) {
        LOG(FATAL) << fmt::format(
                "The ELF '{}' is truncated (program header points after end of file)", path);
    }
    program_headers = reinterpret_cast<const ElfPhdr*>(mapped + program_header_offset);
}

Elf::~Elf() {
    if (mapped) {
        munmap(static_cast<void*>(mapped), elf_size);
    }
    if (_fd > 0) {
        int res = ::close(_fd);
        if (-1 == res) {
            LOG(WARNING) << fmt::format("failed to close {}", _file.native());
        }
        _fd = -1;
    }
}

Elf::Section::Section(const ElfShdr& header_, const Elf& elf_) : header(header_), elf(elf_) {}

bool Elf::iterateSections(std::function<bool(const Section& section, size_t idx)>&& pred) const {
    for (size_t idx = 0; idx < header->e_shnum; ++idx) {
        Section section(section_headers[idx], *this);

        /// Sections spans after end of file.
        if (section.header.sh_offset + section.header.sh_size > elf_size) {
            continue;
        }

        if (pred(section, idx)) {
            return true;
        }
    }
    return false;
}

std::optional<Elf::Section> Elf::findSection(
        std::function<bool(const Section& section, size_t idx)>&& pred) const {
    std::optional<Elf::Section> result;

    iterateSections([&](const Section& section, size_t idx) {
        if (pred(section, idx)) {
            result.emplace(section);
            return true;
        }
        return false;
    });

    return result;
}

std::optional<Elf::Section> Elf::findSectionByName(const char* name) const {
    return findSection(
            [&](const Section& section, size_t) { return 0 == strcmp(name, section.name()); });
}

std::string Elf::getBuildID() const {
    /// Section headers are the first choice for a debuginfo file
    if (std::string build_id; iterateSections([&build_id](const Section& section, size_t) {
            if (section.header.sh_type == SHT_NOTE) {
                build_id = Elf::getBuildID(section.begin(), section.size());
                if (!build_id.empty()) {
                    return true;
                }
            }
            return false;
        })) {
        return build_id;
    }

    /// fallback to PHDR
    for (size_t idx = 0; idx < header->e_phnum; ++idx) {
        const ElfPhdr& phdr = program_headers[idx];

        if (phdr.p_type == PT_NOTE) {
            return getBuildID(mapped + phdr.p_offset, phdr.p_filesz);
        }
    }

    return {};
}

#if defined(OS_SUNOS)
std::string Elf::getBuildID(const char* nhdr_pos, size_t size) {
    return {};
}
#else
std::string Elf::getBuildID(const char* nhdr_pos, size_t size) {
    const char* nhdr_end = nhdr_pos + size;

    while (nhdr_pos < nhdr_end) {
        ElfNhdr nhdr = unaligned_load<ElfNhdr>(nhdr_pos);

        nhdr_pos += sizeof(ElfNhdr) + nhdr.n_namesz;
        if (nhdr.n_type == NT_GNU_BUILD_ID) {
            const char* build_id = nhdr_pos;
            return {build_id, nhdr.n_descsz};
        }
        nhdr_pos += nhdr.n_descsz;
    }

    return {};
}
#endif // OS_SUNOS

std::string Elf::getStoredBinaryHash() const {
    if (auto section = findSectionByName(".clickhouse.hash")) {
        return {section->begin(), section->end()};
    } else {
        return {};
    }
}

const char* Elf::Section::name() const {
    if (!elf.section_names) {
        LOG(FATAL) << fmt::format("Section names are not initialized");
    }

    /// TODO buffer overflow is possible, we may need to check strlen.
    return elf.section_names + header.sh_name;
}

const char* Elf::Section::begin() const {
    return elf.mapped + header.sh_offset;
}

const char* Elf::Section::end() const {
    return begin() + size();
}

size_t Elf::Section::size() const {
    return header.sh_size;
}

} // namespace doris

#endif
