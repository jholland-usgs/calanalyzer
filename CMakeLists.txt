cmake_minimum_required(VERSION 2.8.11)
project(calseek)

set(DIR_SRC ${CMAKE_CURRENT_LIST_DIR})
message("calseek code directory:" ${DIR_SRC})

# ----- COMPILER FLAGS ----- #
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++14 -pthreads -Wall -Qunused-arguments")
