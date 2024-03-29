BRAINVISA_COPY_FILES( ${PROJECT_NAME}-dev "${CMAKE_CURRENT_SOURCE_DIR}/aimsgui.tpl"
                      SOURCE_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
                      DESTINATION "${AIMS_RELATIVE_SIP_DIRECTORY}" 
                      IMMEDIATE )

# Copy typessub.py as  typessub_pyaimsgui.py name to avoid erasing the one from pyaims
configure_file( "${CMAKE_CURRENT_SOURCE_DIR}/typessub.py"
                "${CMAKE_BINARY_DIR}/${AIMS_RELATIVE_SIP_DIRECTORY}/typessub_pyaimsgui.py"
                COPYONLY )
BRAINVISA_INSTALL( PROGRAMS "${CMAKE_BINARY_DIR}/${AIMS_RELATIVE_SIP_DIRECTORY}/typessub_pyaimsgui.py"
                   DESTINATION "${AIMS_RELATIVE_SIP_DIRECTORY}"
                   COMPONENT ${PROJECT_NAME}-dev )

if( SIP_MODULE )
  set( extra_defs "-D" "SIP_MODULE=\"${SIP_MODULE}\"" )
endif()
if( SIP_ABI )
  string( REGEX MATCH "[^.]*" SIP_ABI_MAJOR_VERSION "${SIP_ABI}")
  set( extra_defs "${extra_defs}" "-D"
       "SIP_ABI_MAJOR_VERSION=${SIP_ABI_MAJOR_VERSION}")
endif()

if( QT_MOC_EXECUTABLE )
  set( MT_MOC_OPTS "-m" "${QT_MOC_EXECUTABLE}" )
endif()

exec_program( ${PYTHON_HOST_EXECUTABLE}
  ARGS "${CMAKE_BINARY_DIR}/${AIMS_RELATIVE_SIP_DIRECTORY}/maketemplates.py"
        -i "${CMAKE_CURRENT_SOURCE_DIR}/generatedtypes.py"
        -p "${CMAKE_CURRENT_SOURCE_DIR}"
        -o "${AIMS-FREE_SIP_DIRECTORY}"
        -t "${CMAKE_BINARY_DIR}/${AIMS_RELATIVE_SIP_DIRECTORY}/typessub.py"
        -t "${CMAKE_BINARY_DIR}/${AIMS_RELATIVE_SIP_DIRECTORY}/typessub_pyaimsgui.py"
        -l
        ${MT_MOC_OPTS}
        -P "'${CMAKE_C_PREPROCESSOR}'"
        ${extra_defs}
  OUTPUT_VARIABLE _generatedSipFileList 
  RETURN_VALUE _returnValue )

if( NOT _returnValue EQUAL 0 )
  message( SEND_ERROR "The sip generation from pyaimsgui templates with \"${AIMS-FREE_SIP_DIRECTORY}/maketemplates.py\" returned an error:\n${_generatedSipFileList}\n========================")
endif( NOT _returnValue EQUAL 0 )

file( GLOB _sipSources "${CMAKE_CURRENT_SOURCE_DIR}/*.sip" )

BRAINVISA_ADD_SIP_PYTHON_MODULE( aimsguisip 
  soma/aims
  "${CMAKE_BINARY_DIR}/${AIMS_RELATIVE_SIP_DIRECTORY}/aimsgui_VOID.sip"
  SIP_SOURCES ${_generatedSipFileList} ${_sipSources}
  SIP_INCLUDE  "${AIMS-FREE_SIP_DIRECTORY}" "${PYQT${DESIRED_QT_VERSION}_SIP_DIR}"
  SIP_INSTALL "${AIMS_RELATIVE_SIP_DIRECTORY}" )
target_link_libraries( aimsguisip ${AIMS_PYAIMS_LIBRARIES} aimsgui )

BRAINVISA_INSTALL( TARGETS aimsguisip 
                   DESTINATION ${PYTHON_INSTALL_DIRECTORY}/soma/aims
                   COMPONENT ${PROJECT_NAME} )
