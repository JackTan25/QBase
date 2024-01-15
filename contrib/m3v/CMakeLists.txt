cmake_minimum_required(VERSION 3.5)
project(m3v VERSION 0.1.0)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

set(PostgreSQL_ADDITIONAL_VERSIONS "16")
find_package(PostgreSQL REQUIRED COMPONENTS Server)
if(PostgreSQL_FOUND)
    message(STATUS "PostgreSQL_INCLUDE_DIRS: ${PostgreSQL_INCLUDE_DIRS}")
endif()

# Add Control and SQL Files
set(EXT_CONTROL_FILE ${PROJECT_NAME}.control)
set(EXT_SQL_FILE ${PROJECT_NAME}--1.0.sql)

# Add Compile Options
add_definitions(-DPLATFORM_UNIX)
# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -msse4.2 -maes -mavx2 -fPIC")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fPIC")
set(CMAKE_C_FLAGS "${CMAKE_CXX_FLAGS} -mmwaitx ${CMAKE_EXE_LINKER_FLAGS} -fPIC")
message(STATUS "linker: ${CMAKE_EXE_LINKER_FLAGS}")
# Add Source Files
add_library(${PROJECT_NAME} SHARED
    src/m3v.cpp 
    src/m3vbuild.cpp 
    src/m3vinsert.cpp 
    src/m3vscan.cpp 
    src/m3vutils.cpp 
    src/m3vvaccum.cpp
    src/vector.cpp
)

target_compile_features(${PROJECT_NAME} PUBLIC cxx_std_14)
# target_compile_options(${PROJECT_NAME} PRIVATE -Wall -mavx2)
target_compile_options(${PROJECT_NAME} PRIVATE -Wall)
set_target_properties(${PROJECT_NAME} PROPERTIES CXX_EXTENSIONS OFF)
set_target_properties(${PROJECT_NAME} PROPERTIES PREFIX "")
set_target_properties(${PROJECT_NAME} PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_include_directories(${PROJECT_NAME} PUBLIC /data/include/postgresql/server)
target_include_directories(${PROJECT_NAME} PUBLIC /home/jack/cpp_workspace/wrapdir/OneDb2/contrib/m3v/src/m3v.h 
/home/jack/cpp_workspace/wrapdir/OneDb2/contrib/m3v/src/vector.h)
if(WIN32)
    message(STATUS "WIN32")
    target_include_directories(${PROJECT_NAME} PUBLIC ${PostgreSQL_TYPE_INCLUDE_DIR}/port/win32)
    target_link_libraries(${PROJECT_NAME} PUBLIC ${PostgreSQL_LIBRARY_DIRS}/postgres.lib)
    if(MSVC)
        target_include_directories(${PROJECT_NAME} PUBLIC ${PostgreSQL_TYPE_INCLUDE_DIR}/port/win32_msvc)
        target_compile_options(${PROJECT_NAME} PRIVATE /TC)
    endif(MSVC)
endif(WIN32)

set(PostgreSQL_PG_CONFIG  /data/bin/pg_config)
if (NOT PostgreSQL_PG_CONFIG)
    message(FATAL_ERROR "Unable to find 'pg_config'")
endif ()
message(STATUS "Using pg_config ${PostgreSQL_PG_CONFIG}")

## $> pg_config --sharedir
## /data/share/postgresql
execute_process(
        COMMAND ${PostgreSQL_PG_CONFIG} --sharedir
        OUTPUT_VARIABLE PostgreSQL_SHAREDIR
        OUTPUT_STRIP_TRAILING_WHITESPACE)
execute_process(
        COMMAND ${PostgreSQL_PG_CONFIG} --pkglibdir
        OUTPUT_VARIABLE PostgreSQL_PKGLIBDIR
        OUTPUT_STRIP_TRAILING_WHITESPACE)

message(STATUS "CMAKE_CURRENT_SOURCE_DIR: ${CMAKE_CURRENT_SOURCE_DIR}")
message(STATUS "EXTENSION_FILE: ${CMAKE_CURRENT_SOURCE_DIR}/${EXT_CONTROL_FILE}")
message(STATUS "SQL_FILE: ${CMAKE_CURRENT_SOURCE_DIR}/${EXT_SQL_FILE}")
message(STATUS "DESTINATION: ${PostgreSQL_SHAREDIR}/extension")

install(
    FILES ${CMAKE_CURRENT_SOURCE_DIR}/${EXT_CONTROL_FILE}
    DESTINATION "${PostgreSQL_SHAREDIR}/extension")

install(
    FILES ${CMAKE_CURRENT_SOURCE_DIR}/${EXT_SQL_FILE}
    DESTINATION "${PostgreSQL_SHAREDIR}/extension")


install(
    TARGETS ${PROJECT_NAME}
    DESTINATION ${PostgreSQL_PKGLIBDIR})

message(STATUS "PROJECT_VERSION: ${PROJECT_VERSION}")
message(STATUS "PROJECT_NAME: ${PROJECT_NAME}")
message(STATUS "EXT_CONTROL_FILE: ${EXT_CONTROL_FILE}")
message(STATUS "EXT_SQL_FILE: ${EXT_SQL_FILE}")
message (STATUS "UNIX: CMAKE_CXX_FLAGS: ${CMAKE_CXX_FLAGS}")
message (STATUS "UNIX: CMAKE_C_FLAGS: ${CMAKE_C_FLAGS}")
message(STATUS "PostgreSQL_SHAREDIR: ${PostgreSQL_SHAREDIR}")
message(STATUS "PostgreSQL_PKGLIBDIR: ${PostgreSQL_PKGLIBDIR}")

# cmake -DCMAKE_INSTALL_PREFIX=/data/data -DLIBRARYONLY=ON -DSEEK_ENABLE_TESTS=ON -DCMAKE_BUILD_TYPE=Release ..
# make -j$(nproc)
# make install