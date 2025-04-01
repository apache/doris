// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#include <cstring>
#include <iostream>

#include "logger_impl.h"
#include "windows_customizations.h"

namespace diskann
{

#ifdef ENABLE_CUSTOM_LOGGER
DISKANN_DLLEXPORT ANNStreamBuf coutBuff(stdout);
DISKANN_DLLEXPORT ANNStreamBuf cerrBuff(stderr);

DISKANN_DLLEXPORT std::basic_ostream<char> cout(&coutBuff);
DISKANN_DLLEXPORT std::basic_ostream<char> cerr(&cerrBuff);
std::function<void(LogLevel, const char *)> g_logger;

void SetCustomLogger(std::function<void(LogLevel, const char *)> logger)
{
    g_logger = logger;
    diskann::cout << "Set Custom Logger" << std::endl;
}

ANNStreamBuf::ANNStreamBuf(FILE *fp)
{
    if (fp == nullptr)
    {
        throw diskann::ANNException("File pointer passed to ANNStreamBuf() cannot be null", -1);
    }
    if (fp != stdout && fp != stderr)
    {
        throw diskann::ANNException("The custom logger only supports stdout and stderr.", -1);
    }
    _fp = fp;
    _logLevel = (_fp == stdout) ? LogLevel::LL_Info : LogLevel::LL_Error;
    _buf = new char[BUFFER_SIZE + 1]; // See comment in the header

    std::memset(_buf, 0, (BUFFER_SIZE) * sizeof(char));
    setp(_buf, _buf + BUFFER_SIZE - 1);
}

ANNStreamBuf::~ANNStreamBuf()
{
    sync();
    _fp = nullptr; // we'll not close because we can't.
    delete[] _buf;
}

int ANNStreamBuf::overflow(int c)
{
    std::lock_guard<std::mutex> lock(_mutex);
    if (c != EOF)
    {
        *pptr() = (char)c;
        pbump(1);
    }
    flush();
    return c;
}

int ANNStreamBuf::sync()
{
    std::lock_guard<std::mutex> lock(_mutex);
    flush();
    return 0;
}

int ANNStreamBuf::underflow()
{
    throw diskann::ANNException("Attempt to read on streambuf meant only for writing.", -1);
}

int ANNStreamBuf::flush()
{
    const int num = (int)(pptr() - pbase());
    logImpl(pbase(), num);
    pbump(-num);
    return num;
}
void ANNStreamBuf::logImpl(char *str, int num)
{
    str[num] = '\0'; // Safe. See the c'tor.
    // Invoke the OLS custom logging function.
    if (g_logger)
    {
        g_logger(_logLevel, str);
    }
}
#else
using std::cerr;
using std::cout;
#endif

} // namespace diskann
