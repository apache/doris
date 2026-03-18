#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
# fast-compile-fe.sh — Incrementally compile FE and update doris-fe.jar
#
# Usage:
#   ./fast-compile-fe.sh              # Automatically detect changed files and compile
#   ./fast-compile-fe.sh Foo.java Bar.java  # Specify files to compile
#
# Dependencies: javac, jar (JDK 8+), mvn (needed for initial classpath generation)

set -euo pipefail

DORIS_HOME="$(cd "$(dirname "$0")/.." && pwd)"
FE_CORE="$DORIS_HOME/fe/fe-core"
SRC_ROOT="$FE_CORE/src/main/java"
TARGET_CLASSES="$FE_CORE/target/classes"
TARGET_LIB="$FE_CORE/target/lib"
OUTPUT_JAR="$DORIS_HOME/output/fe/lib/doris-fe.jar"
TARGET_JAR="$FE_CORE/target/doris-fe.jar"
CP_CACHE="$FE_CORE/target/fast-compile-cp.txt"

# Generated source directories (protobuf/thrift/annotation processor generated java files)
GEN_SOURCES=(
    "$FE_CORE/target/generated-sources/doris"
    "$FE_CORE/target/generated-sources/org"
    "$FE_CORE/target/generated-sources/java"
    "$FE_CORE/target/generated-sources/annotations"
    "$FE_CORE/target/generated-sources/antlr4"
)

# ─── Color Output ────────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
info()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*" >&2; }

# ─── Environment Check ───────────────────────────────────────────────────────────
check_env() {
    if [[ ! -d "$TARGET_CLASSES" ]]; then
        error "target/classes does not exist, please run a full build first: cd $DORIS_HOME && mvn package -pl fe/fe-core -DskipTests -T4"
        exit 1
    fi
    if [[ ! -f "$OUTPUT_JAR" && ! -f "$TARGET_JAR" ]]; then
        error "doris-fe.jar does not exist, please run a full build first"
        exit 1
    fi
}

# ─── Find a jar in local .m2 repository (for 'provided' scope deps absent from target/lib) ──
# Usage: find_m2_jar <groupId_path> <artifactId> [version_property_name]
# Example: find_m2_jar org/projectlombok lombok lombok.version
find_m2_jar() {
    local group_path="$1" artifact="$2" version_prop="${3:-}"
    local m2_dir="$HOME/.m2/repository/$group_path/$artifact"
    [[ -d "$m2_dir" ]] || return 0

    if [[ -n "$version_prop" ]]; then
        local version
        version=$(grep "<${version_prop}>" "$DORIS_HOME/fe/pom.xml" 2>/dev/null \
            | sed 's/.*>\(.*\)<.*/\1/' | head -1)
        if [[ -n "$version" ]]; then
            local jar="$m2_dir/${version}/${artifact}-${version}.jar"
            [[ -f "$jar" ]] && echo "$jar" && return
        fi
    fi
    # Fallback: pick the latest version found in .m2
    find "$m2_dir" -name "${artifact}-*.jar" ! -name "*sources*" ! -name "*tests*" \
        2>/dev/null | sort -V | tail -1
}

# ─── Get classpath (with cache) ────────────────────────────────────────────────
get_classpath() {
    # If pom.xml is newer than the cache, regenerate classpath
    if [[ ! -f "$CP_CACHE" || ! -s "$CP_CACHE" || "$FE_CORE/pom.xml" -nt "$CP_CACHE" ]]; then
        info "Generating classpath cache..."
        # Use target/lib directly (dependencies from the latest full build, more reliable than .m2 repository)
        find "$TARGET_LIB" -name "*.jar" | tr '\n' ':' | sed 's/:$//' > "$CP_CACHE"
        info "Classpath cache saved to $CP_CACHE"
    fi

    # 'provided' scope jars are absent from target/lib; locate them from .m2 and append explicitly.
    # lombok: annotation processor for @Getter/@Setter/@Data etc., used widely across the codebase
    local lombok_jar
    lombok_jar="$(find_m2_jar org/projectlombok lombok lombok.version)"
    [[ -z "$lombok_jar" ]] && warn "lombok jar not found; files with Lombok annotations may fail to compile"

    # lakesoul-io-java: directly imported by LakeSoul catalog source files
    local lakesoul_jar
    lakesoul_jar="$(find_m2_jar com/dmetasoul lakesoul-io-java)"

    local provided_cp=""
    for jar in "$lombok_jar" "$lakesoul_jar"; do
        [[ -n "$jar" ]] && provided_cp="$provided_cp:$jar"
    done

    # classpath = dependency jars + provided jars + target/classes (internal project dependencies)
    echo "$(cat "$CP_CACHE")${provided_cp}:$TARGET_CLASSES"
}

# ─── Find stale java files ─────────────────────────────────────────────────────
find_stale_java_files() {
    local stale_files=()

    while IFS= read -r java_file; do
        # java_file: /path/to/src/main/java/org/apache/doris/Foo.java
        # Convert to class file path
        local rel_path="${java_file#$SRC_ROOT/}"       # org/apache/doris/Foo.java
        local class_path="$TARGET_CLASSES/${rel_path%.java}.class"

        if [[ ! -f "$class_path" ]]; then
            # class file does not exist, must compile
            stale_files+=("$java_file")
        elif [[ "$java_file" -nt "$class_path" ]]; then
            # java file is newer than main class file
            stale_files+=("$java_file")
        fi
    done < <(find "$SRC_ROOT" -name "*.java")

    printf '%s\n' "${stale_files[@]}"
}

# ─── Compile java files ───────────────────────────────────────────────────────
compile_files() {
    local classpath="$1"
    shift
    local java_files=("$@")

    # Build source path (including generated source directories)
    local source_path="$SRC_ROOT"
    for gen_src in "${GEN_SOURCES[@]}"; do
        [[ -d "$gen_src" ]] && source_path="$source_path:$gen_src"
    done

    info "Compiling ${#java_files[@]} files..."
    for f in "${java_files[@]}"; do
        echo "  → ${f#$DORIS_HOME/}"
    done

    # Compile with javac
    javac \
        -source 8 -target 8 \
        -encoding UTF-8 \
        -cp "$classpath" \
        -sourcepath "$source_path" \
        -d "$TARGET_CLASSES" \
        "${java_files[@]}" 2>&1

    info "Compilation finished"
}

# ─── Collect class files to update jar ─────────────────────────────────────────
collect_updated_classes() {
    local java_files=("$@")
    local class_files=()

    for java_file in "${java_files[@]}"; do
        local rel_path="${java_file#$SRC_ROOT/}"
        local class_prefix="$TARGET_CLASSES/${rel_path%.java}"
        local dir
        dir="$(dirname "$class_prefix")"
        local base
        base="$(basename "$class_prefix")"

        # Main class file
        [[ -f "$class_prefix.class" ]] && class_files+=("$class_prefix.class")

        # Inner classes and anonymous classes: Foo$Bar.class, Foo$1.class, etc.
        while IFS= read -r inner; do
            class_files+=("$inner")
        done < <(find "$dir" -maxdepth 1 -name "${base}\$*.class" 2>/dev/null)
    done

    printf '%s\n' "${class_files[@]}"
}

# ─── Update jar ───────────────────────────────────────────────────────────────
update_jar() {
    local class_files=("$@")

    info "Updating jar (total ${#class_files[@]} class files)..."

    # Convert class file paths to relative paths for jar command
    local tmpfile
    tmpfile="$(mktemp)"
    trap "rm -f $tmpfile" EXIT

    for cf in "${class_files[@]}"; do
        echo "${cf#$TARGET_CLASSES/}" >> "$tmpfile"
    done

    # Run jar uf in TARGET_CLASSES directory to ensure correct jar internal paths
    pushd "$TARGET_CLASSES" > /dev/null

    # Update target/doris-fe.jar
    if [[ -f "$TARGET_JAR" ]]; then
        xargs jar uf "$TARGET_JAR" < "$tmpfile"
        info "Updated $TARGET_JAR"
    fi

    # Update output/fe/lib/doris-fe.jar
    if [[ -f "$OUTPUT_JAR" ]]; then
        xargs jar uf "$OUTPUT_JAR" < "$tmpfile"
        info "Updated $OUTPUT_JAR"
    fi

    popd > /dev/null
}

# ─── Main workflow ────────────────────────────────────────────────────────────
main() {
    check_env

    local java_files=()

    if [[ $# -gt 0 ]]; then
        # User directly specifies files
        for arg in "$@"; do
            # Support relative and absolute paths
            local abs_path
            if [[ "$arg" = /* ]]; then
                abs_path="$arg"
            else
                abs_path="$(pwd)/$arg"
            fi
            if [[ ! -f "$abs_path" ]]; then
                # Try searching under SRC_ROOT
                local found
                found="$(find "$SRC_ROOT" -name "$(basename "$arg")" | head -1)"
                if [[ -z "$found" ]]; then
                    error "File does not exist: $arg"
                    exit 1
                fi
                abs_path="$found"
            fi
            java_files+=("$abs_path")
        done
        info "Manually specified ${#java_files[@]} files"
    else
        # Automatically detect changes
        info "Scanning for changed Java files..."
        while IFS= read -r f; do
            [[ -n "$f" ]] && java_files+=("$f")
        done < <(find_stale_java_files)

        if [[ ${#java_files[@]} -eq 0 ]]; then
            info "No files need to be compiled, everything is up to date"
            exit 0
        fi
        info "Found ${#java_files[@]} files need to be recompiled"
    fi

    local start_time
    start_time=$(date +%s)

    local classpath
    classpath="$(get_classpath)"

    compile_files "$classpath" "${java_files[@]}"

    # Collect class files (including inner classes)
    local class_files=()
    while IFS= read -r cf; do
        [[ -n "$cf" ]] && class_files+=("$cf")
    done < <(collect_updated_classes "${java_files[@]}")

    if [[ ${#class_files[@]} -eq 0 ]]; then
        warn "No compiled artifacts found, skipping jar update"
        exit 0
    fi

    # Update jar
    update_jar "${class_files[@]}"

    local end_time
    end_time=$(date +%s)
    info "Done! Time elapsed: $((end_time - start_time)) seconds"
}

main "$@"
