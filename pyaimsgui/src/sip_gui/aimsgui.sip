
%Module soma.aims.aimsguisip

%ModuleHeaderCode
#include <aims/qtmisc/resolvesymlinks.h>
%End

%Import aims.sip
%Import qt/qtmod.sip

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


