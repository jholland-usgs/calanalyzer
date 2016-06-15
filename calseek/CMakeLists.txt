cmake_minimum_required(VERSION 2.8.11)
project(calseek)

set(DIR_SRC ${CMAKE_CURRENT_LIST_DIR})
message("calseek code directory:" ${DIR_SRC})

include_directories(${DIR_SRC}/inc/)

add_executable(calseek ${DIR_SRC}/src/calibration.cpp
                    ${DIR_SRC}/src/calseek.cpp)

set_target_properties(calseek
                      PROPERTIES
                      RUNTIME_OUTPUT_DIRECTORY ${DIR_SRC}/bin/)

message("calseek bin directory:" ${DIR_SRC}/bin/)

# ----- COMPILER FLAGS ----- #
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -Wall")