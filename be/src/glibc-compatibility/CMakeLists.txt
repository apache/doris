if (GLIBC_COMPATIBILITY)
    enable_language(ASM)
    include(CheckIncludeFile)

    check_include_file("sys/random.h" HAVE_SYS_RANDOM_H)

    if(COMPILER_CLANG)
        set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wno-builtin-requires-header")
    endif()

    if (CMAKE_VERSION VERSION_GREATER_EQUAL "3.12")
        macro(add_glob cur_list)
            file(GLOB __tmp RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} CONFIGURE_DEPENDS ${ARGN})
            list(APPEND ${cur_list} ${__tmp})
        endmacro()
    else ()
        macro(add_glob cur_list)
            file(GLOB __tmp RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} ${ARGN})
            list(APPEND ${cur_list} ${__tmp})
        endmacro()
    endif ()

    macro(add_headers_and_sources prefix common_path)
        add_glob(${prefix}_headers ${CMAKE_CURRENT_SOURCE_DIR} ${common_path}/*.h)
        add_glob(${prefix}_sources ${common_path}/*.cpp ${common_path}/*.c ${common_path}/*.h)
    endmacro()

    macro(add_headers_only prefix common_path)
        add_glob(${prefix}_headers ${CMAKE_CURRENT_SOURCE_DIR} ${common_path}/*.h)
    endmacro()

    add_headers_and_sources(glibc_compatibility .)
    add_headers_and_sources(glibc_compatibility musl)
    if (ARCH_ARM)
        list (APPEND glibc_compatibility_sources musl/aarch64/syscall.s musl/aarch64/longjmp.s)
        set (musl_arch_include_dir musl/aarch64)
        set (MEMCPY_SOURCE memcpy/memcpy_aarch64.cpp)
        list(REMOVE_ITEM glibc_compatibility_sources musl/getauxval.c)
    elseif (ARCH_AMD64)
        list (APPEND glibc_compatibility_sources musl/x86_64/syscall.s musl/x86_64/longjmp.s)
        set (musl_arch_include_dir musl/x86_64)
        set (MEMCPY_SOURCE memcpy/memcpy_x86_64.cpp)
    else ()
        message (FATAL_ERROR "glibc_compatibility can only be used on x86_64 or aarch64.")
    endif ()

    list(REMOVE_ITEM glibc_compatibility_sources musl/getentropy.c)
    if(HAVE_SYS_RANDOM_H)
        list(APPEND glibc_compatibility_sources musl/getentropy.c)
    endif()

    # Need to omit frame pointers to match the performance of glibc
    set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fomit-frame-pointer")

    # NOTE(amos): getrandom is hard to interpose since we build thirdparty deps
    # without glibc-compatibility. Also sanitizers might generate getrandom
    # libcalls. Workaround: Use object file so that linker will always take a
    # look at its symbol table.
    list(REMOVE_ITEM glibc_compatibility_sources musl/getrandom.c)
    # NOTE(amos): sanitizers might generate memcpy references that are too late to
    # refer. Let's also extract memcpy definitions explicitly to avoid UNDEF GLIBC 2.14.
    add_library(glibc-compatibility-explicit OBJECT musl/getrandom.c ${MEMCPY_SOURCE})
    target_compile_options(glibc-compatibility-explicit PRIVATE -fPIC)
    add_library(glibc-compatibility STATIC ${glibc_compatibility_sources})

    if (COMPILER_CLANG)
        target_compile_options(
            glibc-compatibility
            PRIVATE
            -Wno-unused-command-line-argument
            -Wno-unused-but-set-variable
            -Wno-unused-macros
            -Wno-conversion
            )
    elseif (COMPILER_GCC)
        target_compile_options(
            glibc-compatibility
            PRIVATE
            -Wno-unused-but-set-variable
            -Wno-implicit-fallthrough
            -Wno-maybe-uninitialized
            )
    endif ()

    target_include_directories(glibc-compatibility PRIVATE ${musl_arch_include_dir})
    target_include_directories(glibc-compatibility-explicit PRIVATE ${musl_arch_include_dir})

    message (STATUS "Some symbols from glibc will be replaced for compatibility")
else()
    message (STATUS "not------- compatibility")
endif ()
