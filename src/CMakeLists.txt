add_subdirectory( lib )
add_subdirectory( aimsqtformats )
if( APPLE )
  add_subdirectory( bundle_aimsqtformats )
endif( APPLE )

#------------------------------------------------------------------------------
# Build targets for commands by looking into "Aims*" directories
#------------------------------------------------------------------------------
file( GLOB _commands "${CMAKE_CURRENT_SOURCE_DIR}/Aims*" )
foreach( _command ${_commands} )
  if( IS_DIRECTORY "${_command}" )
  file( GLOB _pro "${_command}/*.pro" )
  BRAINVISA_GET_FILE_LIST_FROM_PRO( "${_pro}"
                              TARGET _target
                              HEADERS _proHeaders
                              SOURCES _proSources )
  set( _headers )
  foreach( _f ${_proHeaders} )
    set( _headers ${_headers} ${_command}/${_f} )
  endforeach( _f ${_proHeaders} )
  set( _sources )
  foreach( _f ${_proSources} )
    set( _sources ${_sources} ${_command}/${_f} )
  endforeach( _f ${_proSources} )
  BRAINVISA_ADD_MOC_FILES( _sources ${_headers} )
  add_executable( ${_target} ${_sources} )
  target_link_libraries( ${_target} aimsgui )
  BRAINVISA_INSTALL( TARGETS ${_target}
                DESTINATION bin
                COMPONENT aimsdata )
  endif( IS_DIRECTORY "${_command}" )
  endforeach( _command ${commands} )
