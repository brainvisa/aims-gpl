BRAINVISA_ADD_SIP_PYTHON_MODULE( aimsguisip soma/aims aimsgui.sip )
target_link_libraries( aimsguisip ${PYTHON_LIBRARY} cartobase cartodata graph aimsdata aimsgui pyaims )
