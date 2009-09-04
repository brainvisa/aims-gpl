BRAINVISA_GET_FILE_LIST_FROM_PRO(  ${CMAKE_CURRENT_SOURCE_DIR}/lib-aimsgui.pro "HEADERS" _headers "SOURCES" _sources )
BRAINVISA_COPY_AND_INSTALL_HEADERS( _headers aims headersTarget )
BRAINVISA_ADD_MOC_FILES( _sources ${_headers} )
add_library( aimsgui SHARED ${_sources} )
add_dependencies( aimsgui ${headersTarget} )
set_property( TARGET aimsgui PROPERTY VERSION ${${PROJECT_NAME}_VERSION} )
target_link_libraries( aimsgui ${QT_LIBRARIES} ${AIMS-FREE_LIBRARIES} )
BRAINVISA_INSTALL( TARGETS aimsgui
              DESTINATION lib
              COMPONENT ${PROJECT_NAME} )
