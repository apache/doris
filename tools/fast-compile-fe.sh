#!/usr/bin/env bash
# fast-compile-fe.sh — 增量编译 FE 并更新 doris-fe.jar
#
# 用法:
#   ./fast-compile-fe.sh              # 自动检测改动文件并编译
#   ./fast-compile-fe.sh Foo.java Bar.java  # 指定编译某些文件
#
# 依赖: javac, jar（JDK 8+），mvn（首次获取 classpath 时需要）

set -euo pipefail

DORIS_HOME="$(cd "$(dirname "$0")/.." && pwd)"
FE_CORE="$DORIS_HOME/fe/fe-core"
SRC_ROOT="$FE_CORE/src/main/java"
TARGET_CLASSES="$FE_CORE/target/classes"
TARGET_LIB="$FE_CORE/target/lib"
OUTPUT_JAR="$DORIS_HOME/output/fe/lib/doris-fe.jar"
TARGET_JAR="$FE_CORE/target/doris-fe.jar"
CP_CACHE="$FE_CORE/target/fast-compile-cp.txt"

# 生成的源码目录（protobuf/thrift/annotation processor 生成的 java 文件）
GEN_SOURCES=(
    "$FE_CORE/target/generated-sources/doris"
    "$FE_CORE/target/generated-sources/org"
    "$FE_CORE/target/generated-sources/java"
    "$FE_CORE/target/generated-sources/annotations"
    "$FE_CORE/target/generated-sources/antlr4"
)

# ─── 颜色输出 ────────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
info()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*" >&2; }

# ─── 检查环境 ─────────────────────────────────────────────────────────────────
check_env() {
    if [[ ! -d "$TARGET_CLASSES" ]]; then
        error "target/classes 不存在，请先完整编译一次: cd $DORIS_HOME && mvn package -pl fe/fe-core -DskipTests -T4"
        exit 1
    fi
    if [[ ! -f "$OUTPUT_JAR" && ! -f "$TARGET_JAR" ]]; then
        error "doris-fe.jar 不存在，请先完整编译一次"
        exit 1
    fi
}

# ─── 获取 classpath（带缓存）─────────────────────────────────────────────────
get_classpath() {
    # 如果 pom.xml 比缓存新，则重新生成 classpath
    if [[ ! -f "$CP_CACHE" || ! -s "$CP_CACHE" || "$FE_CORE/pom.xml" -nt "$CP_CACHE" ]]; then
        info "生成 classpath 缓存..."
        # 直接使用 target/lib（本次完整构建产生的依赖，比 .m2 仓库更可靠）
        find "$TARGET_LIB" -name "*.jar" | tr '\n' ':' | sed 's/:$//' > "$CP_CACHE"
        info "classpath 缓存已保存到 $CP_CACHE"
    fi

    # classpath = 依赖 jars + target/classes（项目内部依赖）
    echo "$(cat "$CP_CACHE"):$TARGET_CLASSES"
}

# ─── 找出需要编译的 java 文件 ────────────────────────────────────────────────
find_stale_java_files() {
    local stale_files=()

    while IFS= read -r java_file; do
        # java_file: /path/to/src/main/java/org/apache/doris/Foo.java
        # 转换为 class 文件路径
        local rel_path="${java_file#$SRC_ROOT/}"       # org/apache/doris/Foo.java
        local class_path="$TARGET_CLASSES/${rel_path%.java}.class"

        if [[ ! -f "$class_path" ]]; then
            # class 文件不存在，肯定需要编译
            stale_files+=("$java_file")
        elif [[ "$java_file" -nt "$class_path" ]]; then
            # java 文件比主 class 文件新
            stale_files+=("$java_file")
        fi
    done < <(find "$SRC_ROOT" -name "*.java")

    printf '%s\n' "${stale_files[@]}"
}

# ─── 编译 java 文件 ───────────────────────────────────────────────────────────
compile_files() {
    local classpath="$1"
    shift
    local java_files=("$@")

    # 构建源码路径（包含生成的源码目录）
    local source_path="$SRC_ROOT"
    for gen_src in "${GEN_SOURCES[@]}"; do
        [[ -d "$gen_src" ]] && source_path="$source_path:$gen_src"
    done

    info "编译 ${#java_files[@]} 个文件..."
    for f in "${java_files[@]}"; do
        echo "  → ${f#$DORIS_HOME/}"
    done

    # javac 编译
    javac \
        -source 8 -target 8 \
        -encoding UTF-8 \
        -cp "$classpath" \
        -sourcepath "$source_path" \
        -d "$TARGET_CLASSES" \
        "${java_files[@]}" 2>&1

    info "编译完成"
}

# ─── 收集需要更新到 jar 的 class 文件 ────────────────────────────────────────
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

        # 主 class 文件
        [[ -f "$class_prefix.class" ]] && class_files+=("$class_prefix.class")

        # 内部类和匿名类：Foo$Bar.class, Foo$1.class 等
        while IFS= read -r inner; do
            class_files+=("$inner")
        done < <(find "$dir" -maxdepth 1 -name "${base}\$*.class" 2>/dev/null)
    done

    printf '%s\n' "${class_files[@]}"
}

# ─── 更新 jar ─────────────────────────────────────────────────────────────────
update_jar() {
    local class_files=("$@")

    info "更新 jar（共 ${#class_files[@]} 个 class 文件）..."

    # 将 class 文件路径转为相对于 TARGET_CLASSES 的路径，供 jar 命令使用
    local tmpfile
    tmpfile="$(mktemp)"
    trap "rm -f $tmpfile" EXIT

    for cf in "${class_files[@]}"; do
        echo "${cf#$TARGET_CLASSES/}" >> "$tmpfile"
    done

    # 在 TARGET_CLASSES 目录下执行 jar uf，使 jar 内路径正确
    pushd "$TARGET_CLASSES" > /dev/null

    # 更新 target/doris-fe.jar
    if [[ -f "$TARGET_JAR" ]]; then
        xargs jar uf "$TARGET_JAR" < "$tmpfile"
        info "已更新 $TARGET_JAR"
    fi

    # 更新 output/fe/lib/doris-fe.jar
    if [[ -f "$OUTPUT_JAR" ]]; then
        xargs jar uf "$OUTPUT_JAR" < "$tmpfile"
        info "已更新 $OUTPUT_JAR"
    fi

    popd > /dev/null
}

# ─── 主流程 ───────────────────────────────────────────────────────────────────
main() {
    check_env

    local java_files=()

    if [[ $# -gt 0 ]]; then
        # 用户直接指定文件
        for arg in "$@"; do
            # 支持相对路径和绝对路径
            local abs_path
            if [[ "$arg" = /* ]]; then
                abs_path="$arg"
            else
                abs_path="$(pwd)/$arg"
            fi
            if [[ ! -f "$abs_path" ]]; then
                # 尝试在 SRC_ROOT 下搜索
                local found
                found="$(find "$SRC_ROOT" -name "$(basename "$arg")" | head -1)"
                if [[ -z "$found" ]]; then
                    error "文件不存在: $arg"
                    exit 1
                fi
                abs_path="$found"
            fi
            java_files+=("$abs_path")
        done
        info "手动指定 ${#java_files[@]} 个文件"
    else
        # 自动检测改动
        info "扫描改动的 Java 文件..."
        while IFS= read -r f; do
            [[ -n "$f" ]] && java_files+=("$f")
        done < <(find_stale_java_files)

        if [[ ${#java_files[@]} -eq 0 ]]; then
            info "没有发现需要编译的文件，已是最新状态"
            exit 0
        fi
        info "发现 ${#java_files[@]} 个文件需要重新编译"
    fi

    local start_time
    start_time=$(date +%s)

    # 获取 classpath
    local classpath
    classpath="$(get_classpath)"

    # 编译
    compile_files "$classpath" "${java_files[@]}"

    # 收集 class 文件（含内部类）π
    local class_files=()
    while IFS= read -r cf; do
        [[ -n "$cf" ]] && class_files+=("$cf")
    done < <(collect_updated_classes "${java_files[@]}")

    if [[ ${#class_files[@]} -eq 0 ]]; then
        warn "未找到编译产物，跳过 jar 更新"
        exit 0
    fi

    # 更新 jar
    update_jar "${class_files[@]}"

    local end_time
    end_time=$(date +%s)
    info "完成！耗时 $((end_time - start_time)) 秒"
}

main "$@"
