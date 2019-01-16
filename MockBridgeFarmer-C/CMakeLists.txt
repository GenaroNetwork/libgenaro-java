cmake_minimum_required(VERSION 3.5.0)
project(MockBridgeFarmer VERSION 0.1.0)

SET(DEBUG 1)

IF(APPLE)

IF(DEBUG)
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -gdwarf-2 -g3")
ENDIF()

include_directories(/usr/local/include)
link_directories(/usr/local/lib)

add_executable(MockBridgeFarmer
               main.c
               mockbridge.c
               mockfarmer.c
               libgenaro/rs.c)

target_link_libraries(MockBridgeFarmer json-c nettle microhttpd)

ELSEIF(UNIX)

IF(DEBUG)
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -g3")
ENDIF()

add_executable(MockBridgeFarmer
               main.c
               mockbridge.c
               mockfarmer.c
               libgenaro/rs.c)

target_link_libraries(MockBridgeFarmer json-c nettle microhttpd)

ELSEIF(WIN32)

IF(DEBUG)
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -g")
ENDIF()

add_executable(MockBridgeFarmer
        main.c
        mockbridge.c
        mockfarmer.c
        libgenaro/rs.c)

target_link_libraries(MockBridgeFarmer json-c nettle microhttpd)

ENDIF()
