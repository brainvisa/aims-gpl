SOMA_GET_FILE_LIST_FROM_PRO(  ${CMAKE_CURRENT_SOURCE_DIR}/lib-aimsgui.pro "HEADERS" _headers "SOURCES" _sources )
SOMA_COPY_AND_INSTALL_HEADERS( _headers aims )
SOMA_ADD_MOC_FILES( _sources ${_headers} )
add_library( aimsgui SHARED ${_sources} )
set_property( TARGET aimsgui PROPERTY VERSION ${${PROJECT_NAME}_VERSION} )
target_link_libraries( aimsgui ${QT_LIBRARIES} ${AIMS_LIBRARIES} )
SOMA_INSTALL( TARGETS aimsgui
              DESTINATION lib
              COMPONENT ${PROJECT_NAME} )
