TEMPLATE        = bundle
TARGET          = aimsqsqlgraphformat${BUILDMODEEXT}

#!include ../../config-local

INCBDIR = aims

LIBS            = $(LIBS_AIMSQTFORMATS)

HEADERS =                   \
io/qsqlgraphformat.h        \
io/qsqlgraphformatheader.h  \
plugin/qsqlgraphplugin.h

SOURCES =                       \
io/qsqlgraphformat.cc           \
io/qsqlgraphformatheader.cc     \
plugin/qsqlgraphplugin.cc


