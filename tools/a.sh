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
#   ./fast-compile.sh                        # Auto-detect changed main sources and compile
#   ./fast-compile.sh Foo.java Bar.java      # Compile specified main sources
#   ./fast-compile.sh --test                 # Auto-detect changed test sources and compile
#   ./fast-compile.sh --test FooTest.java    # Compile specified test sources
#
# Dependencies: javac, jar (JDK 8+), mvn (needed for initial classpath generation)

set -euo pipefail

DORIS_HOME="$(cd "$(dirname "$0")/.." && pwd)"
FE_CORE="$DORIS_HOME/fe/fe-core"
SRC_ROOT="$FE_CORE/src/main/java"
TEST_SRC_ROOT="$FE_CORE/src/test/java"
TARGET_CLASSES="$FE_CORE/target/classes"
TARGET_TEST_CLASSES="$FE_CORE/target/test-classes"
TARGET_LIB="$FE_CORE/target/lib"
OUTPUT_JAR="$DORIS_HOME/output/fe/lib/doris-fe.jar"
TARGET_JAR="$FE_CORE/target/doris-fe.jar"
CP_CACHE="$FE_CORE/target/fast-compile-cp.txt"
TEST_CP_CACHE="$FE_CORE/target/fast-compile-test-cp.txt"

# Generated source directories — auto-scanned so new directories are picked up automatically
GEN_SOURCES=()
for _d in "$FE_CORE/target/generated-sources"/*/; do
    [[ -d "$_d" ]] && GEN_SOURCES+=("${_d%/}")
done
unset _d

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
    # Sanity check: a complete build should have thousands of class files.
    # If only a few hundred exist, the IDE (e.g. VS Code Java extension) likely
    # cleaned and partially rebuilt target/classes.
    local class_count
    class_count=$(find "$TARGET_CLASSES" -name "*.class" | wc -l | tr -d ' ')
    if [[ "$class_count" -lt 1000 ]]; then
        error "target/classes appears incomplete (only $class_count .class files found)."
        error "This usually means the VS Code Java extension has overwritten the Maven build output."
        error "Please run a full build: cd $DORIS_HOME && mvn package -pl fe/fe-core -DskipTests -T4"
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
    # Build the classpath from output/fe/lib/*.jar — always present after a full build.
    # This avoids the 'mvn dependency:build-classpath' approach which fails when the
    # installed POMs in .m2 contain unresolved ${revision} CI-friendly placeholders.
    local output_lib="$DORIS_HOME/output/fe/lib"
    if [[ ! -d "$output_lib" ]]; then
        error "output/fe/lib does not exist, please run a full build first"
        exit 1
    fi

    if [[ ! -f "$CP_CACHE" || ! -s "$CP_CACHE" || "$FE_CORE/pom.xml" -nt "$CP_CACHE" ]]; then
        info "Building classpath from output/fe/lib/*.jar ..."
        local cp=""
        for jar in "$output_lib"/*.jar; do
            [[ -f "$jar" ]] || continue
            cp="${cp:+$cp:}$jar"
        done
        if [[ -z "$cp" ]]; then
            error "No jars found in $output_lib"
            exit 1
        fi
        echo "$cp" > "$CP_CACHE"
        info "Classpath cache saved to $CP_CACHE"
    fi

    # 'provided' scope jars are excluded by the build output; locate from .m2 and append.
    # lombok: annotation processor for @Getter/@Setter/@Data etc., used widely across the codebase
    local lombok_jar
    lombok_jar="$(find_m2_jar org/projectlombok lombok lombok.version)"
    [[ -z "$lombok_jar" ]] && warn "lombok jar not found; files with Lombok annotations may fail to compile"

    # lakesoul-io-java: directly imported by LakeSoul catalog source files
    local lakesoul_jar
    lakesoul_jar="$(find_m2_jar com/dmetasoul lakesoul-io-java)"

    # hudi-common: contains FileIOUtils imported by vendored DiskMap.java
    local hudi_jar
    hudi_jar="$(find_m2_jar org/apache/hudi hudi-common hudi.version)"
    [[ -z "$hudi_jar" ]] && warn "hudi-common jar not found; Hudi source files may fail to compile"

    # immutables/value: @Value.Immutable annotation used by FlightAuthResult.java
    local immutables_jar
    immutables_jar="$(find_m2_jar org/immutables value immutables.version)"
    [[ -z "$immutables_jar" ]] && warn "immutables value jar not found; files using @Value.Immutable may fail to compile"

    local provided_cp=""
    for jar in "$lombok_jar" "$lakesoul_jar" "$hudi_jar" "$immutables_jar"; do
        [[ -n "$jar" ]] && provided_cp="$provided_cp:$jar"
    done

    # classpath = output jars + provided jars + target/classes
    echo "$(cat "$CP_CACHE")${provided_cp}:$TARGET_CLASSES"
}

# ─── Get test classpath (with cache) ──────────────────────────────────────────
get_test_classpath() {
    # Build test classpath from output/fe/lib/*.jar plus test-scope jars from .m2
    local output_lib="$DORIS_HOME/output/fe/lib"
    if [[ ! -f "$TEST_CP_CACHE" || ! -s "$TEST_CP_CACHE" || "$FE_CORE/pom.xml" -nt "$TEST_CP_CACHE" ]]; then
        info "Building test classpath from output/fe/lib/*.jar ..."
        local cp=""
        for jar in "$output_lib"/*.jar; do
            [[ -f "$jar" ]] || continue
            cp="${cp:+$cp:}$jar"
        done

        # Add test-scope jars from .m2 (junit, mockito, etc.)
        local test_jars=()
        local m2="$HOME/.m2/repository"
        for pattern in \
            "org/junit/jupiter" "org/junit/platform" "org/junit/vintage" \
            "org/mockito" "junit/junit" "org/hamcrest" \
            "org/opentest4j" "org/apiguardian"; do
            while IFS= read -r jar; do
                test_jars+=("$jar")
            done < <(find "$m2/$pattern" -name "*.jar" ! -name "*sources*" ! -name "*javadoc*" 2>/dev/null)
        done
        for jar in "${test_jars[@]}"; do
            cp="$cp:$jar"
        done

        echo "$cp" > "$TEST_CP_CACHE"
        info "Test classpath cache saved to $TEST_CP_CACHE"
    fi
    # test classpath = output jars + test jars + main classes + test classes
    echo "$(cat "$TEST_CP_CACHE"):$TARGET_CLASSES:$TARGET_TEST_CLASSES"
}

# ─── Find stale test java files ────────────────────────────────────────────────
find_stale_test_java_files() {
    local stale_files=()

    while IFS= read -r java_file; do
        local rel_path="${java_file#$TEST_SRC_ROOT/}"
        local class_path="$TARGET_TEST_CLASSES/${rel_path%.java}.class"

        if [[ ! -f "$class_path" ]]; then
            stale_files+=("$java_file")
        elif [[ "$java_file" -nt "$class_path" ]]; then
            stale_files+=("$java_file")
        fi
    done < <(find "$TEST_SRC_ROOT" -name "*.java")

    printf '%s\n' "${stale_files[@]}"
}

# ─── Compile test java files ──────────────────────────────────────────────────
compile_test_files() {
    local classpath="$1"
    shift
    local java_files=("$@")

    mkdir -p "$TARGET_TEST_CLASSES"

    info "Compiling ${#java_files[@]} test files..."
    for f in "${java_files[@]}"; do
        echo "  → ${f#$DORIS_HOME/}"
    done

    javac \
        --release 8 \
        -encoding UTF-8 \
        -cp "$classpath" \
        -d "$TARGET_TEST_CLASSES" \
        "${java_files[@]}" 2>&1

    info "Test compilation finished"
}

# ─── Find stale java files ─────────────────────────────────────────────────────
find_stale_java_files() {
    local stale_files=()

    while IFS= read -r java_file; do
        # java_file: /path/to/src/main/java/org/apache/doris/Foo.java
        # Skip the pattern generator package: it is compiled by Maven with <proc>only>
        # (annotation processing only, no class output) and then excluded from
        # default-compile, so no .class file is ever produced for it intentionally.
        [[ "$java_file" == */nereids/pattern/generator/* ]] && continue

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

    info "Compiling ${#java_files[@]} files..."
    for f in "${java_files[@]}"; do
        echo "  → ${f#$DORIS_HOME/}"
    done

    # Compile with javac.
    # NOTE: We intentionally omit -sourcepath here. With -sourcepath, javac would
    # transitively recompile every referenced source file, pulling in files that
    # depend on 'provided'-scope JARs (e.g. com.sleepycat.je, guava, log4j) not
    # present in the incremental classpath.  Since a full build has already
    # populated target/classes, internal project dependencies are resolved from
    # there via -cp, which is sufficient for incremental compilation.
    javac \
        --release 8 \
        -encoding UTF-8 \
        -cp "$classpath" \
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

    # ── Test mode: --test [files...] ──────────────────────────────────────────
    if [[ "${1:-}" == "--test" ]]; then
        shift
        local java_files=()

        if [[ $# -gt 0 ]]; then
            for arg in "$@"; do
                local abs_path
                if [[ "$arg" = /* ]]; then
                    abs_path="$arg"
                else
                    abs_path="$(pwd)/$arg"
                fi
                if [[ ! -f "$abs_path" ]]; then
                    local found
                    found="$(find "$TEST_SRC_ROOT" -name "$(basename "$arg")" | head -1)"
                    if [[ -z "$found" ]]; then
                        error "File does not exist: $arg"
                        exit 1
                    fi
                    abs_path="$found"
                fi
                java_files+=("$abs_path")
            done
            info "Manually specified ${#java_files[@]} test files"
        else
            info "Scanning for changed test Java files..."
            while IFS= read -r f; do
                [[ -n "$f" ]] && java_files+=("$f")
            done < <(find_stale_test_java_files)

            if [[ ${#java_files[@]} -eq 0 ]]; then
                info "No test files need to be compiled, everything is up to date"
                exit 0
            fi
            info "Found ${#java_files[@]} test files need to be recompiled"
        fi

        local start_time
        start_time=$(date +%s)

        local test_classpath
        test_classpath="$(get_test_classpath)"

        compile_test_files "$test_classpath" "${java_files[@]}"

        local end_time
        end_time=$(date +%s)
        info "Done! Time elapsed: $((end_time - start_time)) seconds"
        return
    fi

    # ── Main mode ─────────────────────────────────────────────────────────────
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