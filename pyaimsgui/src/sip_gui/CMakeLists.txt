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

exec_program( ${PYTHON_HOST_EXECUTABLE}
  ARGS "${CMAKE_BINARY_DIR}/${AIMS_RELATIVE_SIP_DIRECTORY}/maketemplates.py"
        -i "${CMAKE_CURRENT_SOURCE_DIR}/generatedtypes.py"
        -p "${CMAKE_CURRENT_SOURCE_DIR}"
        -o "${AIMS-FREE_SIP_DIRECTORY}"
        -t "${CMAKE_BINARY_DIR}/${AIMS_RELATIVE_SIP_DIRECTORY}/typessub.py"
        -t "${CMAKE_BINARY_DIR}/${AIMS_RELATIVE_SIP_DIRECTORY}/typessub_pyaimsgui.py"
        -l
        -m "${QT_MOC_EXECUTABLE}"
  OUTPUT_VARIABLE _generatedSipFileList 
  RETURN_VALUE _returnValue )

if( NOT _returnValue EQUAL 0 )
  message( SEND_ERROR "The sip generation from pyaimsgui templates with \"${AIMS-FREE_SIP_DIRECTORY}/maketemplates.py\" returned an error:\n${_generatedSipFileList}\n========================")
endif( NOT _returnValue EQUAL 0 )

BRAINVISA_ADD_SIP_PYTHON_MODULE( aimsguisip 
  soma/aims
  "${CMAKE_BINARY_DIR}/${AIMS_RELATIVE_SIP_DIRECTORY}/aimsgui_VOID.sip"
  SIP_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/aimsgui.sip" ${_generatedSipFileList}
  SIP_INCLUDE  "${AIMS-FREE_SIP_DIRECTORY}" "${PYQT${DESIRED_QT_VERSION}_SIP_DIR}"
  SIP_INSTALL "${AIMS_RELATIVE_SIP_DIRECTORY}" )
target_link_libraries( aimsguisip ${AIMS_PYAIMS_LIBRARIES} aimsgui )

BRAINVISA_INSTALL( TARGETS aimsguisip 
                   DESTINATION python/soma/aims
                   COMPONENT ${PROJECT_NAME} )
