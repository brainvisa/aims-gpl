cmake_minimum_required( VERSION 3.10 )
find_package( brainvisa-cmake REQUIRED )
BRAINVISA_PROJECT()

# FIXME for an unknown reason the version check does not work iv version is 5.0.0 (but it works using 4.6.2 or 5.1.0, 0 seems to be a problem for cmake. So I remove the test for now.
#BRAINVISA_FIND_PACKAGE( aims-free ${aims-gpl_VERSION} REQUIRED )
BRAINVISA_FIND_PACKAGE( aims-free REQUIRED )
if( NOT DESIRED_QT_VERSION )
  find_package( Qt REQUIRED )
endif()
find_package( QtVersion REQUIRED )
if( DESIRED_QT_VERSION EQUAL 4 )
  find_package( Qt4 COMPONENTS QtCore QtGui QtSql REQUIRED )
elseif( DESIRED_QT_VERSION EQUAL 5 )
  find_package( Qt5Core REQUIRED )
  find_package( Qt5Widgets REQUIRED )
  find_package( Qt5Sql REQUIRED )
elseif( DESIRED_QT_VERSION EQUAL 6 )
  find_package( Qt6 REQUIRED COMPONENTS Widgets Sql )
endif()
find_package( Doxygen )
find_package( PyQt${DESIRED_QT_VERSION} REQUIRED )

if( DESIRED_QT_VERSION EQUAL 4 )
  BRAINVISA_DEPENDENCY( RUN DEPENDS libqtcore4 RUN ">= ${QT_VERSION}" )
  BRAINVISA_DEPENDENCY( DEV DEPENDS libqtcore4 DEV )
  BRAINVISA_DEPENDENCY( RUN DEPENDS libqtgui4 RUN ">= ${QT_VERSION}" )
  BRAINVISA_DEPENDENCY( DEV DEPENDS libqtgui4 DEV )
endif()

BRAINVISA_DEPENDENCY( DEV DEPENDS "${BRAINVISA_PACKAGE_NAME}" RUN "= ${${BRAINVISA_PACKAGE_NAME}_VERSION}" )
BRAINVISA_DEPENDENCY( RUN DEPENDS aims-free RUN "= ${aims-free_VERSION}" )
BRAINVISA_DEPENDENCY( DEV DEPENDS aims-free DEV )

if( DESIRED_QT_VERSION EQUAL 4)
  include( "${QT_USE_FILE}" )
elseif( DESIRED_QT_VERSION EQUAL 5 )
  add_definitions( ${Qt5Widgets_DEFINITIONS} ${Qt5Sql_DEFINITIONS} )
  include_directories( ${Qt5Widgets_INCLUDE_DIRS} ${Qt5Sql_INCLUDE_DIRS} )
  set( QT_LIBRARIES ${Qt5Widgets_LIBRARIES} ${Qt5Sql_LIBRARIES} )
elseif( DESIRED_QT_VERSION EQUAL 6 )
  add_definitions( ${Qt6Widgets_DEFINITIONS} ${Qt6Sql_DEFINITIONS} )
  include_directories( ${Qt6Widgets_INCLUDE_DIRS} ${Qt6Sql_INCLUDE_DIRS} )
  set( QT_LIBRARIES ${Qt6Widgets_LIBRARIES} ${Qt6Sql_LIBRARIES} )
endif()
include( "${AIMS-FREE_USE_FILE}" )

# don't warn on obsolete classes within this library: we still provide obsolete
# features so we have to build them without warnings
add_definitions( "-DAIMSDATA_CLASS_NO_DEPREC_WARNING=1" )

add_subdirectory( pyaimsgui )

BRAINVISA_CREATE_CMAKE_CONFIG_FILES()
