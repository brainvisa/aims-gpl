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

#include <aims/io/qsqlgraphformat.h>
#include <aims/io/qsqlgraphformatheader.h>
#include <aims/io/finder.h>
#include <aims/io/aimsGraphR.h>
#include <graph/graph/graph.h>
#include <cartobase/object/pythonreader.h>
#include <qsqldatabase.h>
#include <qsqlquery.h>
#include <qsqlerror.h>
#include <qvariant.h>

using namespace aims;
using namespace carto;
using namespace std;

bool FinderQSqlGraphFormat::check( const string & filename, Finder & f ) const
{
  QSqlGraphFormatHeader *hdr = new QSqlGraphFormatHeader( filename );
  try
    {
      hdr->read();
    }
  catch( exception & e )
    {
      delete hdr;
      return( false );
    }
  f.setObjectType( "Graph" );
  string        format, datatype;
  vector<string> possibledatatypes;
  hdr->getProperty( "file_type", format );
  hdr->getProperty( "data_type", datatype );
  hdr->getProperty( "possible_data_type", possibledatatypes );
  f.setFormat( format );
  f.setDataType( datatype );
  f.setPossibleDataTypes( possibledatatypes );
  f.setHeader( hdr );

  return true;
}


// ---


namespace
{

  map<string, map<string, vector<string> > > *attributesSyntax
  ( QSqlDatabase & db, const string & fileName )
  {
    QSqlQuery res = db.exec( "SELECT soma_class, name, type, label, optional,"
      " default_value FROM soma_attributes" );
    if( db.lastError().isValid() )
      throw syntax_check_error( db.lastError().text().utf8().data(),
                                fileName );
    typedef map<string, map<string, vector<string> > > ResType;
    ResType *attributes = new ResType;
    bool ok = false, optional;
    while( res.next() )
    {
      string t = res.value(0).toString().utf8().data();
      ResType::iterator i = attributes->find( t );
      if( i == attributes->end() )
      {
        (*attributes)[t] = map<string, vector<string> >();
        i = attributes->find( t );
      }
      map<string, vector<string> > & gt = i->second;
      optional = res.value(4).toInt( &ok );
      if( !ok )
        optional = true;
      vector<string> & sem = gt[ res.value(1).toString().utf8().data() ];
      sem.reserve( 4 );
      sem.push_back( res.value(2).toString().utf8().data() );
      sem.push_back( res.value(3).toString().utf8().data() );
      sem.push_back( optional ? "" : "needed" );
      sem.push_back( res.value(5).toString().utf8().data() );
    }
    return attributes;
  }


  Object intHelper( const QVariant & res, bool & ok )
  {
    int value = res.toInt( &ok );
    if( ok )
      return Object::value( value );
    return none();
  }


  Object floatHelper( const QVariant & res, bool & ok )
  {
    float value = res.toFloat( &ok );
    if( ok )
      return Object::value( value );
    return none();
  }


  Object stringHelper( const QVariant & res, bool & ok )
  {
    if( res.isNull() )
    {
      ok = false;
      return none();
    }
    ok = true;
    string value = res.toString().utf8().data();
    return Object::value( value );
  }


  Object pythonHelper( const QVariant & res, bool & ok )
  {
    if( res.isNull() )
    {
      ok = false;
      return none();
    }
    string value = res.toString().utf8().data();
    ok = true;
    istringstream sst( value );
    PythonReader pr;
    pr.attach( sst );
    return pr.read( 0, "" );
  }


  Object pythonHelperWithSyntax( const QVariant & res, bool & ok,
                                 const string & stype )
  {
    if( res.isNull() )
    {
      ok = false;
      return none();
    }
    string value = res.toString().utf8().data();
    ok = true;
    istringstream sst( value );
    SyntaxSet ss;
    ss[ "__generic__" ][ "__fallback__" ] = Semantic( stype, false );
    PythonReader pr( ss );
    pr.attach( sst );
    return pr.read( 0, "__fallback__" );
  }


  Object intVectorHelper( const QVariant & res, bool & ok )
  {
    return pythonHelperWithSyntax( res, ok, "int_vector" );
  }


  Object floatVectorHelper( const QVariant & res, bool & ok )
  {
    return pythonHelperWithSyntax( res, ok, "float_vector" );
  }


  Object stringVectorHelper( const QVariant & res, bool & ok )
  {
    return pythonHelperWithSyntax( res, ok, "string_vector" );
  }


  Object typedValue( const QVariant & res, const string & att,
                     const vector<string> & sem, string & attname, bool & ok )
  {
    Object value;
    ok = false;

    typedef Object (*HelperFunc)( const QVariant &, bool & );
    static map<string, HelperFunc> semanticTypes;
    if( semanticTypes.empty() )
    {
      semanticTypes[ "Int()" ] = intHelper;
      semanticTypes[ "Float()" ] = floatHelper;
      semanticTypes[ "Double()" ] = floatHelper;
      semanticTypes[ "String()" ] = stringHelper;
      semanticTypes[ "List()" ] = pythonHelper;
      semanticTypes[ "IntVector()" ] = intVectorHelper;
      semanticTypes[ "FloatVector()" ] = floatVectorHelper;
      semanticTypes[ "StringVector()" ] = stringVectorHelper;
      semanticTypes[ "Dictionary()" ] = pythonHelper;
    }
    const string & type = sem[0];
    map<string, HelperFunc>::const_iterator ihelper
      = semanticTypes.find( type );
    if( ihelper != semanticTypes.end() )
      value = ihelper->second( res, ok );
    else
      value = stringHelper( res, ok );
    if( !sem[1].empty() )
      attname = sem[1];
    else
      attname = att;
    return value;
  }


  void fillItem( GenericObject* item, QSqlQuery & res,
                 const map<string, vector<string> > & vatts, int i )
  {
    map<string, vector<string> >::const_iterator ia, ea = vatts.end();
    for( ia=vatts.begin(); ia!=ea; ++ia, ++i )
    {
      const string & att = ia->first;
      const vector<string> & sem = ia->second;
      string attname;
      bool ok;
      Object value = typedValue( res.value(i), att, sem, attname, ok );
      if( ok )
        item->setProperty( attname, value );
    }
  }

}

// -------

Graph* QSqlGraphFormat::read( const std::string & filename,
                              const carto::AllocatorContext & context,
                              carto::Object options )
{
  QSqlGraphFormatHeader *hdr = new QSqlGraphFormatHeader( filename );
  try
  {
    hdr->read();
  }
  catch( exception & e )
  {
    cout << "exception: " << e.what() << endl;
    delete hdr;
    throw;
  }
  string syntax;
  hdr->getProperty( "arg_syntax", syntax );
  Graph *graph = new Graph( syntax );
  graph->setProperty( "header", Object( static_cast<GenericObject *>( hdr ) ) );
  if( !read( filename, *graph, context, options ) )
    throw invalid_format_error( "SQL graph reading failed", filename );
  return graph;
}


bool QSqlGraphFormat::read( const std::string & filename1, Graph & graph,
                            const carto::AllocatorContext &,
                            carto::Object options )
{
  Object hdr;
  int gid = -1;
  string syntax, sqldbtype, filename;
  try
  {
    hdr = graph.getProperty( "header" );
    graph.removeProperty( "header" );
  }
  catch( exception & )
  {
    QSqlGraphFormatHeader *h = new QSqlGraphFormatHeader( filename1 );
    try
    {
      h->read();
    }
    catch( exception & e )
    {
      delete h;
      throw;
    }
    hdr = Object( static_cast<GenericObject *>( h ) );
  }
  hdr->getProperty( "arg_syntax", syntax );
  hdr->getProperty( "graph_sql_eid", gid );
  hdr->getProperty( "sql_database_type", sqldbtype );
  filename = static_cast<QSqlGraphFormatHeader *>( hdr.get() )->name();

  ostringstream osgid;
  osgid << gid;
  string sgid = osgid.str();

  // (re-)open DB connection
  try
  {
#if QT_VERSION >= 0x040000
    QSqlDatabase db = QSqlDatabase::addDatabase( sqldbtype.c_str(),
                                                filename.c_str() );
#else
    QSqlDatabase *pdb = QSqlDatabase::addDatabase( sqldbtype.c_str(),
                                                  filename.c_str() );
    QSqlDatabase & db = *pdb;
#endif
    db.setDatabaseName( filename.c_str() );
    bool ok = db.open();
    if( !ok )
    {
      throw wrong_format_error( db.lastError().text().utf8().data(),
                                filename );
    }

    // read syntax in DB
    rc_ptr<map<string, map<string, vector<string> > > > syntaxattributes(
      attributesSyntax( db, filename ) );
    map<string, map<string, vector<string> > > & attributes = *syntaxattributes;

    // read graph attributes
    typedef map<string, vector<string> > AttsOfType;
    AttsOfType & gatts = attributes[ syntax ];
    string sql = "SELECT ";
    AttsOfType::iterator ia, ea = gatts.end();
    int x = 0;
    for( ia=gatts.begin(); ia!=ea; ++ia, ++x )
    {
      if( x != 0 )
        sql += ", ";
      sql += ia->first;
    }
    sql += " FROM " + syntax + " WHERE eid=" + sgid;
    QSqlQuery res = db.exec( sql.c_str() );
    if( res.lastError().isValid() )
      throw wrong_format_error( res.lastError().text().utf8().data(),
                                filename );

    res.next();
    fillItem( &graph, res, gatts, 0 );

    // retreive vertices types
    sql = "SELECT class_name FROM class JOIN _Vertex ON _Vertex.eid=class.eid"
      " WHERE _Vertex.graph=" + sgid + " GROUP BY class_name";
    res = db.exec( sql.c_str() );
    if( res.lastError().isValid() )
      throw wrong_format_error( res.lastError().text().utf8().data(),
                                filename );
    list<string> vtypes;
    while( res.next() )
      vtypes.push_back( res.value(0).toString().utf8().data() );
    map<int, Vertex *> id2vertex;

    // select vertices
    list<string>::iterator ivt, evt = vtypes.end();
    for( ivt=vtypes.begin(); ivt!=evt; ++ivt )
    {
      const string & vt = *ivt;
      const map<string, vector<string> > & vatts = attributes[ vt ];
      sql = "SELECT eid";
      map<string, vector<string> >::const_iterator ivat, evat = vatts.end();
      for( ivat=vatts.begin(); ivat!=evat; ++ivat )
        sql += string( ", " ) + ivat->first;
      sql += " FROM " + vt + " WHERE graph=" + sgid;
      res = db.exec( sql.c_str() );
      if( res.lastError().isValid() )
        throw wrong_format_error( res.lastError().text().utf8().data(),
                                  filename );
      while( res.next() )
      {
        Vertex *v = graph.addVertex( vt );
        int vid = res.value(0).toInt();
        id2vertex[ vid ] = v;
        fillItem( v, res, vatts, 1 );
      }
    }

    // retreive edges types
    sql = "SELECT class_name FROM class JOIN _Vertex,_Edge ON "
      "_Edge.eid=class.eid AND _Edge.eid=class.eid WHERE "
      "(_Vertex.eid=_Edge.vertex1 OR _Vertex.eid=_Edge.vertex2) AND "
      "_Vertex.graph=" + sgid + " GROUP BY class_name";
    res = db.exec( sql.c_str() );
    if( res.lastError().isValid() )
      throw wrong_format_error( res.lastError().text().utf8().data(),
                                filename );
    list<string> etypes;
    while( res.next() )
      etypes.push_back( res.value(0).toString().utf8().data() );

    // select edges
    evt = etypes.end();
    for( ivt=etypes.begin(); ivt!=evt; ++ivt )
    {
      const string & et = *ivt;
      const map<string, vector<string> > & eatts = attributes[ et ];
      sql = "SELECT vertex1, vertex2";
      map<string, vector<string> >::const_iterator ieat, eeat = eatts.end();
      for( ieat=eatts.begin(); ieat!=eeat; ++ieat )
        sql += string( ", " ) + ieat->first;
      sql += " FROM " + et + " JOIN _Vertex WHERE (_Vertex.eid=vertex1 OR "
        "_Vertex.eid=vertex2) AND _Vertex.graph=" + sgid;
      res = db.exec( sql.c_str() );
      if( res.lastError().isValid() )
        throw wrong_format_error( res.lastError().text().utf8().data(),
                                  filename );
      while( res.next() )
      {
        Vertex *v1 = id2vertex[ res.value(0).toInt() ];
        Vertex *v2 = id2vertex[ res.value(1).toInt() ];
        Edge* e = graph.addEdge( v1, v2, et );
        fillItem( e, res, eatts, 2 );
      }
    }

    db.close();
  }
  catch( exception & )
  {
    QSqlDatabase::removeDatabase( filename.c_str() );
    throw;
  }

  QSqlDatabase::removeDatabase( filename.c_str() );

  // read the .data directory part
  int subobjectsfilter = -1;
  try
  {
    Object filt = options->getProperty( "subobjectsfilter" );
    subobjectsfilter = (int) filt->getScalar();
  }
  catch( ... )
  {
  }

  AimsGraphReader     gr( filename );
  if( subobjectsfilter < 0 )
    gr.readElements( graph, 3 );
  else if( subobjectsfilter > 0 )
    gr.readElements( graph, 1 );
  else
  {
    graph.setProperty( "aims_reader_filename", filename );
    graph.setProperty( "aims_reader_loaded_objects", int(0) );
  }

  return true;
}


bool QSqlGraphFormat::write( const std::string & filename,
                             const Graph & graph, bool )
{

  return false;
}

