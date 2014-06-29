cmake_minimum_required(VERSION 2.8)

find_package(JNI)

if (JNI_FOUND)
    message (STATUS "JNI_INCLUDE_DIRS=${JNI_INCLUDE_DIRS}")
    message (STATUS "JNI_LIBRARIES=${JNI_LIBRARIES}")
endif()

include_directories(${JNI_INCLUDE_DIRS} mysql-hyperloglog/libmysqlhll/ mysql-hyperloglog/libmysqlhll/cpp-hyperloglog/include mysql-hyperloglog/libmysqlhll/base64)

add_library (pighll SHARED pighll.cxx)
