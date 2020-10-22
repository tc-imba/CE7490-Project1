cmake_minimum_required(VERSION 3.10)
project(social_network)

set(CMAKE_CXX_STANDARD 17)

include_directories(snap/snap-core)
include_directories(snap/glib-core)
include_directories(metis/include)

set(CMAKE_CXX_FLAGS "-fopenmp -fpermissive -Wno-unused-result")

add_subdirectory(snap)
add_subdirectory(metis)
add_subdirectory(metis/GKlib)

add_executable(social_network src/main.cpp src/Manager.cpp src/Server.cpp src/MergedGraph.cpp)
target_link_libraries(social_network metis GKlib snap)

add_executable(metis_test src/metis.cpp)
target_link_libraries(metis_test metis GKlib snap)
