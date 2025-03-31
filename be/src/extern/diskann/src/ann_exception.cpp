// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#include "ann_exception.h"
#include <sstream>
#include <string>

namespace diskann
{
ANNException::ANNException(const std::string &message, int errorCode)
    : std::runtime_error(message), _errorCode(errorCode)
{
}

std::string package_string(const std::string &item_name, const std::string &item_val)
{
    return std::string("[") + item_name + ": " + std::string(item_val) + std::string("]");
}

ANNException::ANNException(const std::string &message, int errorCode, const std::string &funcSig,
                           const std::string &fileName, uint32_t lineNum)
    : ANNException(package_string(std::string("FUNC"), funcSig) + package_string(std::string("FILE"), fileName) +
                       package_string(std::string("LINE"), std::to_string(lineNum)) + "  " + message,
                   errorCode)
{
}

FileException::FileException(const std::string &filename, std::system_error &e, const std::string &funcSig,
                             const std::string &fileName, uint32_t lineNum)
    : ANNException(std::string(" While opening file \'") + filename + std::string("\', error code: ") +
                       std::to_string(e.code().value()) + "  " + e.code().message(),
                   e.code().value(), funcSig, fileName, lineNum)
{
}

} // namespace diskann