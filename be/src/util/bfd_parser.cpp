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

#include "util/bfd_parser.h"

#include <memory>

#include "common/logging.h"

namespace doris {

struct BfdFindCtx {
    BfdFindCtx(bfd_symbol** syms_, bfd_vma pc_)
            : found(false),
              syms(syms_),
              pc(pc_),
              file_name(nullptr),
              func_name(nullptr),
              lineno(0) {}

    bool found;
    bfd_symbol** syms;
    bfd_vma pc;
    const char* file_name;
    const char* func_name;
    unsigned int lineno;
};

std::mutex BfdParser::_bfd_mutex;
bool BfdParser::_is_bfd_inited = false;

static void find_addr_in_section(bfd* abfd, asection* sec, void* arg) {
    BfdFindCtx* ctx = (BfdFindCtx*)arg;
    if (ctx->found) {
        return;
    }
#ifdef bfd_get_section_flags
    if ((bfd_get_section_flags(abfd, sec) & SEC_ALLOC) == 0) {
        return;
    }
#else
    if ((bfd_section_flags(sec) & SEC_ALLOC) == 0) {
        return;
    }
#endif
#ifdef bfd_get_section_vma
    auto vma = bfd_get_section_vma(abfd, sec);
#else
    auto vma = bfd_section_vma(sec);
#endif
    if (ctx->pc < vma) {
        return;
    }
#ifdef bfd_get_section_size
    auto size = bfd_get_section_size(sec);
#else
    auto size = bfd_section_size(sec);
#endif
    if (ctx->pc >= vma + size) {
        return;
    }
    ctx->found = bfd_find_nearest_line(abfd, sec, ctx->syms, ctx->pc - vma, &ctx->file_name,
                                       &ctx->func_name, &ctx->lineno);
}

static void section_print(bfd* bfd, asection* sec, void* arg) {
    std::string* str = (std::string*)arg;
    str->append(sec->name);
    str->push_back('\n');
}

void BfdParser::init_bfd() {
    if (_is_bfd_inited) {
        return;
    }
    std::lock_guard<std::mutex> lock(_bfd_mutex);
    bfd_init();
    if (!bfd_set_default_target("elf64-x86-64")) {
        LOG(ERROR) << "set default target to elf64-x86-64 failed.";
    }
    _is_bfd_inited = true;
}

BfdParser* BfdParser::create() {
    FILE* file = fopen("/proc/self/cmdline", "r");
    if (file == nullptr) {
        return nullptr;
    }

    char prog_name[1024];
    // Ignore unused return value
    if (fscanf(file, "%1023s ", prog_name))
        ;
    fclose(file);
    std::unique_ptr<BfdParser> parser(new BfdParser(prog_name));
    if (parser->parse()) {
        return nullptr;
    }
    return parser.release();
}

BfdParser* BfdParser::create(const std::string& prog_name) {
    std::unique_ptr<BfdParser> parser(new BfdParser(prog_name));
    if (parser->parse()) {
        return nullptr;
    }
    return parser.release();
}

void BfdParser::list_targets(std::vector<std::string>* out) {
    if (!_is_bfd_inited) {
        init_bfd();
    }

    const char** targets = bfd_target_list();
    const char** p = targets;
    while ((*p) != nullptr) {
        out->emplace_back(*p++);
    }
    free(targets);
}

BfdParser::BfdParser(const std::string& file_name)
        : _file_name(file_name), _abfd(nullptr), _syms(nullptr), _num_symbols(0), _symbol_size(0) {
    if (!_is_bfd_inited) {
        init_bfd();
    }
}

BfdParser::~BfdParser() {
    if (_syms != nullptr) {
        free(_syms);
    }
    if (_abfd != nullptr) {
        bfd_close(_abfd);
    }
}

void BfdParser::list_sections(std::string* ss) {
    std::lock_guard<std::mutex> lock(_mutex);
    bfd_map_over_sections(_abfd, section_print, (void*)ss);
}

static void list_matching_formats(char** p, std::string* message) {
    if (!p || !*p) {
        return;
    }
    message->append(": Matching formats: ");
    while (*p) {
        message->append(*p++);
    }
    message->push_back('\n');
}

int BfdParser::open_bfd() {
    _abfd = bfd_openr(_file_name.c_str(), nullptr);
    if (_abfd == nullptr) {
        LOG(WARNING) << "bfd_openr failed because errmsg=" << bfd_errmsg(bfd_get_error());
        return -1;
    }
    if (bfd_check_format(_abfd, bfd_archive)) {
        LOG(WARNING) << "bfd_check_format for archive failed because errmsg="
                     << bfd_errmsg(bfd_get_error());
        return -1;
    }

    char** matches = nullptr;
    if (!bfd_check_format_matches(_abfd, bfd_object, &matches)) {
        if (bfd_get_error() == bfd_error_file_ambiguously_recognized) {
            std::string message = _file_name;
            list_matching_formats(matches, &message);
            free(matches);
            LOG(WARNING) << "bfd_check_format_matches failed because errmsg="
                         << bfd_errmsg(bfd_get_error()) << " and " << message;
        } else {
            LOG(WARNING) << "bfd_check_format_matches failed because errmsg="
                         << bfd_errmsg(bfd_get_error());
        }
        return -1;
    }

    return 0;
}

int BfdParser::load_symbols() {
    if ((bfd_get_file_flags(_abfd) & HAS_SYMS) == 0) {
        // No need to load symbols;
        return 0;
    }
    _num_symbols = bfd_read_minisymbols(_abfd, FALSE, (void**)&_syms, &_symbol_size);
    if (_num_symbols == 0) {
        _num_symbols =
                bfd_read_minisymbols(_abfd, TRUE /* dynamic */, (void**)&_syms, &_symbol_size);
    }
    if (_num_symbols == 0) {
        LOG(WARNING) << "Load symbols failed because errmsg=" << bfd_errmsg(bfd_get_error());
        return -1;
    }
    return 0;
}

int BfdParser::parse() {
    int ret = open_bfd();
    if (ret != 0) {
        LOG(WARNING) << "open bfd failed.";
        return ret;
    }

    ret = load_symbols();
    if (ret != 0) {
        LOG(WARNING) << "Load symbols failed.";
        return ret;
    }

    return 0;
}

int BfdParser::decode_address(const char* str, const char** end, std::string* file_name,
                              std::string* func_name, unsigned int* lineno) {
    bfd_vma pc = bfd_scan_vma(str, end, 16);
    BfdFindCtx ctx(_syms, pc);

    std::lock_guard<std::mutex> lock(_mutex);
    bfd_map_over_sections(_abfd, find_addr_in_section, (void*)&ctx);
    if (!ctx.found) {
        file_name->append("??");
        func_name->append("??");
        return -1;
    }
    // demange function
    if (ctx.func_name != nullptr) {
#define DMGL_PARAMS (1 << 0)
#define DMGL_ANSI (1 << 1)
        char* demangled_name = bfd_demangle(_abfd, ctx.func_name, DMGL_ANSI | DMGL_PARAMS);
        if (demangled_name != nullptr) {
            func_name->append(demangled_name);
            free(demangled_name);
        } else {
            func_name->append(ctx.func_name);
        }
    } else {
        func_name->append("??");
    }
    // file_name
    if (ctx.file_name != nullptr) {
        file_name->append(ctx.file_name);
    } else {
        file_name->append("??");
    }
    *lineno = ctx.lineno;
    return 0;
}

} // namespace doris
