cmake_minimum_required( VERSION 2.6 )
find_package( brainvisa-cmake REQUIRED )
BRAINVISA_PROJECT()

find_package( aims-free REQUIRED )
find_package( Qt REQUIRED )
find_package( Doxygen )

set( AIMS-GPL_DEFINITIONS ${AIMS_DEFINITIONS} ${QT_DEFINITIONS} )
add_definitions( ${AIMS-GPL_DEFINITIONS} )

set( AIMS-GPL_INCLUDE_DIRS "${CMAKE_BINARY_DIR}/include" ${AIMS_INCLUDE_DIRS} "${QT_INCLUDE_DIR}" )
include_directories( ${AIMS-GPL_INCLUDE_DIRS} )

add_subdirectory( src )

BRAINVISA_CREATE_CMAKE_CONFIG_FILES()
