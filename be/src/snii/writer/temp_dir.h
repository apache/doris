#pragma once

#include <sys/statvfs.h>

#include <cstdint>
#include <cstdlib>
#include <string>

namespace snii::writer {

// Scratch directory for spill runs and section temp files. Resolution order:
//   SNII_TEMP_DIR (explicit config) -> TMPDIR (POSIX default) -> /tmp (fallback).
//
// Point SNII_TEMP_DIR / TMPDIR at a REAL disk (SSD/NVMe). /tmp is often tmpfs (a
// RAM-backed filesystem) on modern systems, where spilling does NOT reduce RSS --
// it just moves bytes from heap to tmpfs, defeating the purpose of spilling.
inline std::string resolve_temp_dir() {
    for (const char* var : {"SNII_TEMP_DIR", "TMPDIR"}) {
        const char* v = std::getenv(var);
        if (v != nullptr && v[0] != '\0') {
            std::string d(v);
            while (d.size() > 1 && d.back() == '/') d.pop_back(); // strip trailing '/'
            return d;
        }
    }
    return "/tmp";
}

// Best-effort free bytes on the filesystem backing `dir`. Returns UINT64_MAX when
// statvfs fails, so a caller's space pre-check never false-positives on an
// unstattable path. CAVEATS: this is best-effort only -- it is subject to TOCTOU
// (free space can drop before/while the write runs), and on tmpfs it reports
// RAM-backed space (use the temp-dir config to avoid tmpfs in the first place).
inline uint64_t temp_dir_available_bytes(const std::string& dir) {
    struct statvfs vfs;
    if (::statvfs(dir.c_str(), &vfs) != 0) return UINT64_MAX;
    return static_cast<uint64_t>(vfs.f_bavail) * static_cast<uint64_t>(vfs.f_frsize);
}

} // namespace snii::writer
