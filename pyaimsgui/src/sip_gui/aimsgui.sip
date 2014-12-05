
// %Module soma.aims.aimsguisip

%ModuleHeaderCode
#include <aims/qtmisc/resolvesymlinks.h>
#include <cartobase/object/object.h>
#include <aims/qtmisc/initqapplication.h>
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

QCoreApplication* initializeQApplication( SIP_PYOBJECT argv,
                                          bool use_gui=true );
%MethodCode
  if( !PySequence_Check( a0 ) )
  {
    sipIsErr = 1;
    PyErr_SetString( PyExc_TypeError, "wrong argument 1 type" );
  }
  else
  {
    static std::vector<char *> argv;
    static std::vector<std::string> sargv;
    unsigned argc;

    if( argv.empty() )
    {
      unsigned    i;
      argc = PySequence_Size( a0 );
      argv.resize( argc + 1 );
      sargv.resize( argc );
      argv[ argc ] = 0;

      PyObject    *pyitem;
      for( i=0; i<argc; ++i )
      {
        pyitem = PySequence_GetItem(a0,i);
        if( !pyitem || !PyString_Check( pyitem ) )
        {
          if( pyitem )
            Py_DECREF( pyitem );
          sipIsErr = 1;
          sipRes = 0;
          std::ostringstream s;
          s << "wrong list item type in arg 1, item " << i;
          PyErr_SetString( PyExc_TypeError, s.str().c_str() );
          break;
        }

        sargv[i] = std::string( PyString_AsString( pyitem ) );
        argv[i] = const_cast<char *>( sargv[i].c_str() );
        Py_DECREF( pyitem );
      }
    }

    sipRes = aims::initializeQApplication( argv.size() - 1, &argv[0], a1 );
  }

%End


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

