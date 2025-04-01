// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once
#ifdef _WINDOWS
#ifndef USE_BING_INFRA
#include <Windows.h>
#include <fcntl.h>
#include <malloc.h>
#include <minwinbase.h>

#include <cstdio>
#include <mutex>
#include <thread>
#include "aligned_file_reader.h"
#include "tsl/robin_map.h"
#include "utils.h"
#include "windows_customizations.h"

class WindowsAlignedFileReader : public AlignedFileReader
{
  private:
#ifdef UNICODE
    std::wstring m_filename;
#else
    std::string m_filename;
#endif

  protected:
    // virtual IOContext createContext();

  public:
    DISKANN_DLLEXPORT WindowsAlignedFileReader(){};
    DISKANN_DLLEXPORT virtual ~WindowsAlignedFileReader(){};

    // Open & close ops
    // Blocking calls
    DISKANN_DLLEXPORT virtual void open(const std::string &fname) override;
    DISKANN_DLLEXPORT virtual void close() override;

    DISKANN_DLLEXPORT virtual void register_thread() override;
    DISKANN_DLLEXPORT virtual void deregister_thread() override
    {
        // TODO: Needs implementation.
    }
    DISKANN_DLLEXPORT virtual void deregister_all_threads() override
    {
        // TODO: Needs implementation.
    }
    DISKANN_DLLEXPORT virtual IOContext &get_ctx() override;

    // process batch of aligned requests in parallel
    // NOTE :: blocking call for the calling thread, but can thread-safe
    DISKANN_DLLEXPORT virtual void read(std::vector<AlignedRead> &read_reqs, IOContext &ctx, bool async) override;
};
#endif // USE_BING_INFRA
#endif //_WINDOWS
