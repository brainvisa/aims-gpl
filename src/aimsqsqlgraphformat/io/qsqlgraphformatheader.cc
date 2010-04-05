/* This software and supporting documentation are distributed by
 *     Institut Federatif de Recherche 49
 *     CEA/NeuroSpin, Batiment 145,
 *     91191 Gif-sur-Yvette cedex
 *     France
 *
 * This software is governed by the CeCILL license version 2 under
 * French law and abiding by the rules of distribution of free software.
 * You can  use, modify and/or redistribute the software under the
 * terms of the CeCILL license version 2 as circulated by CEA, CNRS
 * and INRIA at the following URL "http://www.cecill.info".
 *
 * As a counterpart to the access to the source code and  rights to copy,
 * modify and redistribute granted by the license, users are provided only
 * with a limited warranty  and the software's author,  the holder of the
 * economic rights,  and the successive licensors  have only  limited
 * liability.
 *
 * In this respect, the user's attention is drawn to the risks associated
 * with loading,  using,  modifying and/or developing or reproducing the
 * software by the user in light of its specific status of free software,
 * that may mean  that it is complicated to manipulate,  and  that  also
 * therefore means  that it is reserved for developers  and  experienced
 * professionals having in-depth computer knowledge. Users are therefore
 * encouraged to load and test the software's suitability as regards their
 * requirements in conditions enabling the security of their systems and/or
 * data to be ensured and,  more generally, to use and operate it in the
 * same conditions as regards security.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL license version 2 and that you accept its terms.
 */

#include <aims/io/qsqlgraphformatheader.h>
#include <cartobase/exception/ioexcept.h>
#include <qsqldatabase.h>
#include <qsqlquery.h>
#include <qsqlerror.h>
#include <qvariant.h>

using namespace aims;
using namespace carto;
using namespace std;


QSqlGraphFormatHeader::QSqlGraphFormatHeader( const string & name ) :
  PythonHeader(),
  _name( name )
{
}


QSqlGraphFormatHeader::~QSqlGraphFormatHeader()
{
}


void QSqlGraphFormatHeader::read()
{
  string fileName = name();
  unsigned n = 0;
  int gid = 0;
  string synt, dbtype = "QSQLITE";

  try
  {
#if QT_VERSION >= 0x040000
    QSqlDatabase db = QSqlDatabase::addDatabase( dbtype.c_str(),
                                                 fileName.c_str() );
#else
    QSqlDatabase *pdb = QSqlDatabase::addDatabase( dbtype.c_str(),
                                                   fileName.c_str() );
    QSqlDatabase & db = *pdb;
#endif
    db.setDatabaseName( fileName.c_str() );
    bool ok = db.open();
    if( !ok )
    {
      if( fileName.size() < 7 || fileName.substr( fileName.length()-7, 7 ) != ".sqlite" )
      {
        fileName += ".sqlite";
        db.setDatabaseName( fileName.c_str() );
        ok = db.open();
        if( ok )
          _name = fileName;
        else
          throw file_not_found_error( db.lastError().text().utf8().data(),
                                      _name );
      }
    }

    try
    {
      // query available graphs in DB
      QSqlQuery res = db.exec( "SELECT Graph.eid, class.class_name FROM Graph"
        " JOIN class on class.eid=Graph.eid" );
      if( db.lastError().isValid() )
        throw syntax_check_error( db.lastError().text().utf8().data(),
                                  fileName );
      // cout << "GRAPHS:\n";
      while( res.next() )
      {
        gid = res.value(0).toInt();
        synt = res.value(1).toString().utf8().data();
        // cout << gid << ": " << synt << endl;
        ++n;
      }
    }
    catch( ... )
    {
      db.close();
      throw;
    }

    db.close();
  }
  catch( ... )
  {
    QSqlDatabase::removeDatabase( fileName.c_str() );
    throw;
  }

  QSqlDatabase::removeDatabase( fileName.c_str() );

  if( n != 1 )
  {
    cout << "database does not contain one graph\n";
    throw invalid_format_error( "database does not contain one graph",
                                fileName );
  }
  setProperty( "file_type", string( "QSqlGraph" ) );
  setProperty( "object_type", string( "Graph" ) );
  setProperty( "data_type", string( "VOID" ) );
  setProperty( "arg_syntax", synt );
  setProperty( "graph_sql_eid", gid );
  setProperty( "sql_database_type", dbtype );

  // add meta-info to header
  readMinf( _name + ".minf" );
}


void QSqlGraphFormatHeader::write()
{
  string fileName = name();

  // write meta-info header
  setProperty( "file_type", string( "SQLGRAPH" ) );
  setProperty( "object_type", string( "Graph" ) );
  setProperty( "data_type", string( "VOID" ) );

  writeMinf( _name + ".minf" );
}


set<string> QSqlGraphFormatHeader::extensions() const
{
  set<string>   exts;
  exts.insert( ".sqlite" );
  return exts;
}

string QSqlGraphFormatHeader::extension() const
{
  set<string> exts = extensions();
  set<string>::iterator ie, ee = exts.end();
  string::size_type l = _name.length(), m;
  for( ie=exts.begin(); ie!=ee; ++ie )
  {
    m = ie->length();
    if( l <= m && _name.substr( l - m, m ) == *ie )
      return *ie;
  }
  return "";
}
