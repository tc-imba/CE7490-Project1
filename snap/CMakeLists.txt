cmake_minimum_required(VERSION 3.10)

set(CMAKE_CXX_STANDARD 17)

include_directories(snap-core)
include_directories(glib-core)

set(CMAKE_CXX_FLAGS "-fopenmp -fpermissive -Wno-unused-result")

file(GLOB GLIB_CORE_FILES glib-core/*.h glib-core/*.cpp)
add_library(snap snap-core/Snap.cpp)
