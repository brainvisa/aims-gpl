BRAINVISA_ADD_SIP_PYTHON_MODULE( aimsguisip soma/aims aimsgui.sip
  SIP_INSTALL "${AIMS_RELATIVE_SIP_DIRECTORY}" )
target_link_libraries( aimsguisip ${PYTHON_LIBRARIES} cartobase cartodata graph aimsdata aimsgui pyaims )
