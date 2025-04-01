// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once
#include <string>
#include <stdexcept>
#include <system_error>
#include <cstdint>

#include "windows_customizations.h"

#ifndef _WINDOWS
#define __FUNCSIG__ __PRETTY_FUNCTION__
#endif

namespace diskann
{

class ANNException : public std::runtime_error
{
  public:
    DISKANN_DLLEXPORT ANNException(const std::string &message, int errorCode);
    DISKANN_DLLEXPORT ANNException(const std::string &message, int errorCode, const std::string &funcSig,
                                   const std::string &fileName, uint32_t lineNum);

  private:
    int _errorCode;
};

class FileException : public ANNException
{
  public:
    DISKANN_DLLEXPORT FileException(const std::string &filename, std::system_error &e, const std::string &funcSig,
                                    const std::string &fileName, uint32_t lineNum);
};
} // namespace diskann
