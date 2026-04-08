#include <cstdarg>
#include <cstdio>

namespace {
void raw_log_impl(int severity, const char* file, int line, const char* format, va_list ap) {
    char buffer[2048];
    std::vsnprintf(buffer, sizeof(buffer), format, ap);
    const char* sev = "INFO";
    if (severity == 1)
        sev = "WARN";
    else if (severity == 2)
        sev = "ERROR";
    else if (severity == 3)
        sev = "FATAL";
    std::fprintf(stderr, "[%s] %s:%d %s\n", sev, file ? file : "", line, buffer);
}
}

extern "C" void rawlog_compat_paimon(int severity, const char* file, int line, const char* format,
                                     ...) __asm__("_ZN6google8RawLog__ENS_11LogSeverityEPKciS2_z");

extern "C" void rawlog_compat_paimon(int severity, const char* file, int line, const char* format,
                                     ...) {
    va_list ap;
    va_start(ap, format);
    raw_log_impl(severity, file, line, format, ap);
    va_end(ap);
}
