
// %Module soma.aims.aimsguisip

%ModuleHeaderCode
#include <aims/qtmisc/resolvesymlinks.h>
#include <cartobase/object/object.h>
%End

// %Import aims.sip
// %Import qt/qtmod.sip

%Include qsqlgraphdatabase.sip
%Include list_CurrentGraphData.sip

namespace aims
{
  bool qtResolveSymlinks();
  void setQtResolveSymlinks( bool );
};

class AttributedView : QMainWindow
{
%TypeHeaderCode
#include <qobject.h>
#include <qmainwindow.h>
#include <aims/attributedview/attributedview.h>
%End

public:
  AttributedView();
  void loadObject( const QString & );
  void load( std::string );
  virtual void closeEvent ( QCloseEvent * );

public slots:
  void loadObject();

};


QWidget* qWidget_fromObject( carto::GenericObject & );
%MethodCode
  try
  {
    sipRes = a0->value<QWidget *>();
  }
  catch( std::exception & e )
  {
    sipIsErr = 1;
    PyErr_SetString( PyExc_RuntimeError, e.what() );
    sipRes = 0;
  }
%End

