cmake_minimum_required(VERSION 3.10)
project(social_network)

set(CMAKE_CXX_STANDARD 14)

include_directories(snap/snap-core)
include_directories(snap/glib-core)

set(CMAKE_CXX_FLAGS "-fopenmp -fpermissive -Wno-unused-result")

file(GLOB GLIB_CORE_FILES snap/glib-core/*.h snap/glib-core/*.cpp)
add_library(snap snap/snap-core/Snap.cpp)


add_executable(social_network main.cpp src/Manager.cpp src/Server.cpp)
target_link_libraries(social_network snap)
