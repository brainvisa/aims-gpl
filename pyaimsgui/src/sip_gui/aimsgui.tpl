%Import aims.sip

// switch to Qt 3 or Qt 4
%#if QT_VERSION >= 0x040000%
%Import QtGui/QtGuimod.sip
%Import QtSql/QtSqlmod.sip
%#if QT_VERSION >= 0x050000%
%Import QtWidgets/QtWidgetsmod.sip
%#endif%
%#else%
%Import qt/qtmod.sip
%Import qtsql/qtsqlmod.sip
%#endif%

%Module soma.aims.aimsguisip

%Include aimsgui.sip

