#include <glog/logging.h>

#include <cstdarg>
#include <cstdio>

namespace google {
void RawLog__(LogSeverity severity, const char* file, int line, const char* format, ...) {
    char buffer[2048];
    va_list ap;
    va_start(ap, format);
    std::vsnprintf(buffer, sizeof(buffer), format, ap);
    va_end(ap);
    const char* sev = "INFO";
    if (severity == GLOG_WARNING)
        sev = "WARN";
    else if (severity == GLOG_ERROR)
        sev = "ERROR";
    else if (severity == GLOG_FATAL)
        sev = "FATAL";
    std::fprintf(stderr, "[%s] %s:%d %s\n", sev, file ? file : "", line, buffer);
}
} // namespace google
