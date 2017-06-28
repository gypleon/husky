# Copyright 2016 Husky Team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


### Eigen ###

# TODO: Check the version of Eigen
set(EIGEN_FIND_REQUIRED true)
find_path(EIGEN_INCLUDE_DIR NAMES eigen3/Eigen/Dense)
set(EIGEN_INCLUDE_DIR ${EIGEN_INCLUDE_DIR}/eigen3)
if(EIGEN_INCLUDE_DIR)
    set(EIGEN_FOUND true)
endif(EIGEN_INCLUDE_DIR)
if(EIGEN_FOUND)
    set(DEP_FOUND true)
    if(NOT EIGEN_FIND_QUIETLY)
        message (STATUS "Found Eigen:")
        message (STATUS "  (Headers)       ${EIGEN_INCLUDE_DIR}")
    endif(NOT EIGEN_FIND_QUIETLY)
else(EIGEN_FOUND)
    set(DEP_FOUND false)
    if(EIGEN_FIND_REQUIRED)
        message(FATAL_ERROR "Could NOT find Eigen")
    endif(EIGEN_FIND_REQUIRED)
endif(EIGEN_FOUND)

### TCMalloc ###

set(TCMALLOC_FIND_REQUIRED true)
find_path(TCMALLOC_INCLUDE_DIR NAMES gperftools/malloc_extension.h)
find_library(TCMALLOC_LIBRARY NAMES tcmalloc)
if(TCMALLOC_LIBRARY)
    set(TCMALLOC_FOUND true)
endif(TCMALLOC_LIBRARY)
if(TCMALLOC_FOUND)
    set(DEP_FOUND true)
    if(NOT TCMALLOC_FIND_QUIETLY)
        message (STATUS "Found TCMalloc:")
        message (STATUS "  (Library)       ${TCMALLOC_LIBRARY}")
    endif(NOT TCMALLOC_FIND_QUIETLY)
else(TCMALLOC_FOUND)
    set(DEP_FOUND false)
    if(TCMALLOC_FIND_REQUIRED)
        message(FATAL_ERROR "Could NOT find TCMalloc")
    endif(TCMALLOC_FIND_REQUIRED)
endif(TCMALLOC_FOUND)

### LibHDFS3 ###

find_path(LIBHDFS3_INCLUDE_DIR NAMES hdfs/hdfs.h)
find_library(LIBHDFS3_LIBRARY NAMES hdfs3)
if(LIBHDFS3_INCLUDE_DIR AND LIBHDFS3_LIBRARY)
    set(LIBHDFS3_FOUND true)
endif(LIBHDFS3_INCLUDE_DIR AND LIBHDFS3_LIBRARY)
if(LIBHDFS3_FOUND)
    set(LIBHDFS3_DEFINITION "-DWITH_HDFS")
    if(NOT LIBHDFS3_FIND_QUIETLY)
        message (STATUS "Found libhdfs3:")
        message (STATUS "  (Headers)       ${LIBHDFS3_INCLUDE_DIR}")
        message (STATUS "  (Library)       ${LIBHDFS3_LIBRARY}")
        message (STATUS "  (Definition)    ${LIBHDFS3_DEFINITION}")
    endif(NOT LIBHDFS3_FIND_QUIETLY)
else(LIBHDFS3_FOUND)
    message(STATUS "Could NOT find libhdfs3")
endif(LIBHDFS3_FOUND)
if(WITHOUT_HDFS)
    unset(LIBHDFS3_FOUND)
    message(STATUS "Not using libhdfs3 due to WITHOUT_HDFS option")
endif(WITHOUT_HDFS)

### MongoDB ###

find_path(MONGOCLIENT_INCLUDE_DIR NAMES mongo)
find_library(MONGOCLIENT_LIBRARY NAMES mongoclient)
if (MONGOCLIENT_INCLUDE_DIR AND MONGOCLIENT_LIBRARY)
    set(MONGOCLIENT_FOUND true)
endif(MONGOCLIENT_INCLUDE_DIR AND MONGOCLIENT_LIBRARY)
if (MONGOCLIENT_FOUND)
    set(MONGOCLIENT_DEFINITION "-DWITH_MONGODB")
    message (STATUS "Found MongoClient:")
    message (STATUS "  (Headers)       ${MONGOCLIENT_INCLUDE_DIR}")
    message (STATUS "  (Library)       ${MONGOCLIENT_LIBRARY}")
    message (STATUS "  (Definition)    ${MONGOCLIENT_DEFINITION}")
else(MONGOCLIENT_FOUND)
    message (STATUS "Could NOT find MongoClient")
endif(MONGOCLIENT_FOUND)
if(WITHOUT_MONGODB)
    unset(MONGOCLIENT_FOUND)
    message(STATUS "Not using MongoClient due to WITHOUT_MONGODB option")
endif(WITHOUT_MONGODB)
    
### Redis ###

if(REDISCLIENT_SEARCH_PATH)
    find_path(REDISCLIENT_INCLUDE_DIR NAMES hiredis PATHS ${REDISCLIENT_SEARCH_PATH})
    find_library(REDISCLIENT_LIBRARY NAMES hiredis PATHS ${REDISCLIENT_SEARCH_PATH})
else(REDISCLIENT_SEARCH_PATH)
    find_path(REDISCLIENT_INCLUDE_DIR NAMES hiredis)
    find_library(REDISCLIENT_LIBRARY NAMES hiredis)
endif(REDISCLIENT_SEARCH_PATH)
if(REDISCLIENT_INCLUDE_DIR AND REDISCLIENT_LIBRARY)
    set(REDISCLIENT_FOUND true)
endif(REDISCLIENT_INCLUDE_DIR AND REDISCLIENT_LIBRARY)
if(REDISCLIENT_FOUND)
    set(REDISCLIENT_DEFINITION "-DWITH_REDIS")
    message (STATUS "Found Hiredis:")
    message (STATUS "  (Headers)       ${REDISCLIENT_INCLUDE_DIR}")
    message (STATUS "  (Library)       ${REDISCLIENT_LIBRARY}")
    message (STATUS "  (Definition)    ${REDISCLIENT_DEFINITION}")
else(REDISCLIENT_FOUND)
    if(WIN32)
        message (STATUS "Redis and hiredis are currently not available on win32")
    else(WIN32)
        message (STATUS "hiredis will be included as a third party:")
        include(ExternalProject)
        set(THIRDPARTY_DIR ${PROJECT_SOURCE_DIR}/third_party)
        if(NOT REDISCLIENT_INCLUDE_DIR OR NOT REDISCLIENT_LIBRARY)
            set(REDIS_INSTALL "cp")
            ExternalProject_Add(
                hiredis
                GIT_REPOSITORY "https://github.com/redis/hiredis"
                GIT_TAG "v0.13.3"
                PREFIX ${THIRDPARTY_DIR}
                UPDATE_COMMAND ""
                CONFIGURE_COMMAND ""
                # TODO: if remove "-pedantic" strict Warnings.
                BUILD_COMMAND sed -i "s/ -pedantic//g" ${THIRDPARTY_DIR}/src/hiredis/Makefile COMMAND make dynamic COMMAND make static
                BUILD_IN_SOURCE 1
                INSTALL_COMMAND mkdir -p ${PROJECT_BINARY_DIR}/include/hiredis/adapters ${PROJECT_BINARY_DIR}/lib COMMAND ${REDIS_INSTALL} ${THIRDPARTY_DIR}/src/hiredis/hiredis.h ${PROJECT_BINARY_DIR}/include/hiredis/hiredis.h COMMAND ${REDIS_INSTALL} ${THIRDPARTY_DIR}/src/hiredis/read.h ${PROJECT_BINARY_DIR}/include/hiredis/read.h COMMAND ${REDIS_INSTALL} ${THIRDPARTY_DIR}/src/hiredis/sds.h ${PROJECT_BINARY_DIR}/include/hiredis/sds.h COMMAND ${REDIS_INSTALL} ${THIRDPARTY_DIR}/src/hiredis/async.h ${PROJECT_BINARY_DIR}/include/hiredis/async.h COMMAND ${REDIS_INSTALL} ${THIRDPARTY_DIR}/src/hiredis/adapters/ae.h ${PROJECT_BINARY_DIR}/include/hiredis/adapters/ae.h COMMAND ${REDIS_INSTALL} ${THIRDPARTY_DIR}/src/hiredis/adapters/libev.h ${PROJECT_BINARY_DIR}/include/hiredis/adapters/libev.h COMMAND ${REDIS_INSTALL} ${THIRDPARTY_DIR}/src/hiredis/adapters/libevent.h ${PROJECT_BINARY_DIR}/include/hiredis/adapters/libevent.h COMMAND ${REDIS_INSTALL} ${THIRDPARTY_DIR}/src/hiredis/libhiredis.so ${PROJECT_BINARY_DIR}/lib/libhiredis.so.0.13 COMMAND ln -sf ${PROJECT_BINARY_DIR}/lib/libhiredis.so.0.13 ${PROJECT_BINARY_DIR}/lib/libhiredis.so.0 COMMAND ln -sf ${PROJECT_BINARY_DIR}/lib/libhiredis.so.0 ${PROJECT_BINARY_DIR}/lib/libhiredis.so COMMAND ${REDIS_INSTALL} ${THIRDPARTY_DIR}/src/hiredis/libhiredis.a ${PROJECT_BINARY_DIR}/lib/libhiredis.a
            )
            list(APPEND external_project_dependencies hiredis)
        endif(NOT REDISCLIENT_INCLUDE_DIR OR NOT REDISCLIENT_LIBRARY)
        set(REDISCLIENT_INCLUDE_DIR "${PROJECT_BINARY_DIR}/include/hiredis")
        if(BUILD_SHARED_LIBRARY)
            set(REDISCLIENT_LIBRARY "${PROJECT_BINARY_DIR}/lib/libhiredis.so")
        else(BUILD_SHARED_LIBRARY)
            set(REDISCLIENT_LIBRARY "${PROJECT_BINARY_DIR}/lib/libhiredis.a")
        endif(BUILD_SHARED_LIBRARY)
        message (STATUS "  (Headers should be)       ${REDISCLIENT_INCLUDE_DIR}")
        message (STATUS "  (Library should be)       ${REDISCLIENT_LIBRARY}")
        set(REDISCLIENT_FOUND true)
        set(REDISCLIENT_DEFINITION "-DWITH_REDIS")
    endif(WIN32)
endif(REDISCLIENT_FOUND)
if(WITHOUT_REDIS)
    unset(REDISCLIENT_FOUND)
    unset(REDISCLIENT_DEFINITION)
    message(STATUS "Not using Hiredis due to WITHOUT_REDIS option")
endif(WITHOUT_REDIS)

### RT ###

find_library(RT_LIBRARY NAMES rt)
if(RT_LIBRARY)
    set(RT_FOUND true)
else(RT_LIBRARY)
    set(RT_FOUND false)
endif(RT_LIBRARY)
if (RT_FOUND)
    message (STATUS "Found RT:")
    message (STATUS "  (Library)       ${RT_LIBRARY}")
else(RT_FOUND)
    message (STATUS "Could NOT find RT")
endif(RT_FOUND)

### Thrift ###

find_path(THRIFT_INCLUDE_DIR NAMES thrift)
find_library(THRIFT_LIBRARY NAMES thrift)
if (THRIFT_INCLUDE_DIR AND THRIFT_LIBRARY)
    set(THRIFT_FOUND true)
endif(THRIFT_INCLUDE_DIR AND THRIFT_LIBRARY)
if (THRIFT_FOUND)
    set(THRIFT_DEFINITION "-DWITH_THRIFT")
    message (STATUS "Found Thrift:")
    message (STATUS "  (Headers)       ${THRIFT_INCLUDE_DIR}")
    message (STATUS "  (Library)       ${THRIFT_LIBRARY}")
    message (STATUS "  (Definition)    ${THRIFT_DEFINITION}")
else(THRIFT_FOUND)
    message (STATUS "Could NOT find Thrift")
endif(THRIFT_FOUND)
if(WITHOUT_THRIFT)
    unset(THRIFT_FOUND)
    message(STATUS "Not using Thrift due to WITHOUT_THRIFT option")
endif(WITHOUT_THRIFT)

### ORC ###

#NAMES liblz4.a liborc.a libprotobuf.a libsnappy.a libz.a 
#NAMES ColumnPrinter.hh Int128.hh MemoryPool.hh orc-config.hh OrcFile.hh Reader.hh Type.hh  Vector.hh
find_path(ORC_INCLUDE_DIR NAMES orc/OrcFile.hh)
find_library(ORC_L0 NAMES protobuf NO_CMAKE_ENVIRONMENT_PATH NO_CMAKE_SYSTEM_PATH NO_SYSTEM_ENVIRONMENT_PATH)
find_library(ORC_L1 NAMES z NO_CMAKE_ENVIRONMENT_PATH NO_CMAKE_SYSTEM_PATH NO_SYSTEM_ENVIRONMENT_PATH)
find_library(ORC_L2 NAMES lz4 NO_CMAKE_ENVIRONMENT_PATH NO_CMAKE_SYSTEM_PATH NO_SYSTEM_ENVIRONMENT_PATH)
find_library(ORC_L3 NAMES snappy NO_CMAKE_ENVIRONMENT_PATH NO_CMAKE_SYSTEM_PATH NO_SYSTEM_ENVIRONMENT_PATH)
find_library(ORC_L4 NAMES orc)

if (ORC_INCLUDE_DIR AND ORC_L1 AND ORC_L0 AND ORC_L2 AND ORC_L3 AND ORC_L4)
    set(ORC_FOUND true)
endif (ORC_INCLUDE_DIR AND ORC_L1 AND ORC_L0 AND ORC_L2 AND ORC_L3 AND ORC_L4)
if (ORC_FOUND)
    set(ORC_DEFINITION "-DWITH_ORC")
    # The order is important for dependencies.
    set(ORC_LIBRARY ${ORC_L4} ${ORC_L3} ${ORC_L2} ${ORC_L1} ${ORC_L0})
    message (STATUS "Found ORC:")
    message (STATUS "  (Headers)       ${ORC_INCLUDE_DIR}")
    message (STATUS "  (Library)       ${ORC_L0}")
    message (STATUS "  (Library)       ${ORC_L1}")
    message (STATUS "  (Library)       ${ORC_L2}")
    message (STATUS "  (Library)       ${ORC_L3}")
    message (STATUS "  (Library)       ${ORC_L4}")
else(ORC_FOUND)
    message (STATUS "Could NOT find ORC")
endif(ORC_FOUND)
if(WITHOUT_ORC)
    unset(ORC_FOUND)
    message(STATUS "Not using ORC due to WITHOUT_ORC option")
endif(WITHOUT_ORC)
