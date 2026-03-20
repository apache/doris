# boost - header-only + minimal compiled libs via bootstrap/b2
set(BOOST_SRC ${TP_SOURCE_DIR}/boost_1_81_0)
set(BOOST_BUILD_DIR ${CMAKE_CURRENT_BINARY_DIR}/boost)

# Boost 1.81 does NOT have a root CMakeLists.txt.
# Use header-only approach and build required libs via b2.
if(NOT EXISTS "${BOOST_BUILD_DIR}/lib/libboost_date_time.a")
    message(STATUS "[contrib] Building boost from source (b2)...")
    file(MAKE_DIRECTORY ${BOOST_BUILD_DIR})
    execute_process(
        COMMAND ${BOOST_SRC}/bootstrap.sh --prefix=${BOOST_BUILD_DIR} --with-libraries=date_time,system,container
        WORKING_DIRECTORY ${BOOST_SRC}
        OUTPUT_QUIET
    )
    include(ProcessorCount)
    ProcessorCount(NPROC)
    execute_process(
        COMMAND ${BOOST_SRC}/b2 install
            --prefix=${BOOST_BUILD_DIR}
            --with-date_time --with-system
            link=static variant=release cxxflags=-fPIC
            -j${NPROC}
        WORKING_DIRECTORY ${BOOST_SRC}
        OUTPUT_QUIET
    )
    message(STATUS "[contrib] boost build complete")
endif()

# Header-only interface for all of Boost
add_library(boost_headers INTERFACE)
target_include_directories(boost_headers INTERFACE ${BOOST_SRC})

# Compiled boost libs
foreach(_lib boost_date_time boost_system)
    add_library(${_lib} STATIC IMPORTED GLOBAL)
    if(EXISTS "${BOOST_BUILD_DIR}/lib/lib${_lib}.a")
        set_target_properties(${_lib} PROPERTIES IMPORTED_LOCATION "${BOOST_BUILD_DIR}/lib/lib${_lib}.a")
    endif()
endforeach()

# boost_container is header-only in most usages
add_library(boost_container INTERFACE)
target_include_directories(boost_container INTERFACE ${BOOST_SRC})

# Alias for Boost::headers
add_library(Boost::headers ALIAS boost_headers)
# Aliases expected by BE
if(TARGET boost_date_time AND NOT TARGET Boost::date_time)
    add_library(Boost::date_time ALIAS boost_date_time)
endif()
if(NOT TARGET Boost::container)
    add_library(Boost::container ALIAS boost_container)
endif()
if(TARGET boost_system AND NOT TARGET Boost::system)
    add_library(Boost::system ALIAS boost_system)
endif()
