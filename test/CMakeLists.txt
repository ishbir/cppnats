cmake_minimum_required(VERSION 3.0)
project(cppnats_test)
enable_testing()

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Werror -std=c++14")

# We need to include OpenSSL and threading because CNATS needs it.
find_package(OpenSSL REQUIRED)
find_package(Threads REQUIRED)

# Add CNATS
include(ExternalProject)
ExternalProject_Add(cnats
  GIT_REPOSITORY "https://github.com/nats-io/cnats.git"
  GIT_TAG "v1.3.6"
  UPDATE_COMMAND "")
  #INSTALL_COMMAND "": Leave the install step as it is.

ExternalProject_Get_Property(cnats source_dir)
set(CNATS_INSTALL_DIR ${source_dir}/../install)

# Add include files
include_directories(${CNATS_INSTALL_DIR}/include)
# Add library
add_library(nats STATIC IMPORTED)
# nats is statically compiled as a library with a different name: nats_static
set_property(TARGET nats PROPERTY IMPORTED_LOCATION
  ${CNATS_INSTALL_DIR}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}nats_static${CMAKE_STATIC_LIBRARY_SUFFIX})
# Make sure that cnats is downloaded and built into a library.
add_dependencies(nats cnats)

# Include cppnats
include_directories(${PROJECT_SOURCE_DIR}/../include)

# Build the actual project.
set(SOURCE_FILES tests.cpp)
add_executable(cppnats_test ${SOURCE_FILES})
target_link_libraries(cppnats_test nats ${OPENSSL_LIBRARIES} ${CMAKE_THREAD_LIBS_INIT})

# Declare tests
add_test(NAME ${PROJECT_NAME} COMMAND $<TARGET_FILE:${PROJECT_NAME}>)
