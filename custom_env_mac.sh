# This file is generated automatically. PLEASE DO NOT MODIFY IT.

HOMEBREW_REPO_PREFIX="/opt/homebrew"
CELLARS=(
    automake
    autoconf
    libtool
    pkg-config
    texinfo
    coreutils
    gnu-getopt
    python@3
    cmake
    ninja
    ccache
    bison
    byacc
    gettext
    wget
    pcre
    maven
    llvm@16
)
for cellar in "${CELLARS[@]}"; do
    EXPORT_CELLARS="${HOMEBREW_REPO_PREFIX}/opt/${cellar}/bin:${EXPORT_CELLARS}"
done
export PATH="${EXPORT_CELLARS}:/usr/bin:${PATH}"

export DORIS_BUILD_PYTHON_VERSION=python3
