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
#include <aims/io/aimsGraphW.h>
#include <aims/def/path.h>
#include <graph/graph/graph.h>
#include <cartobase/object/pythonreader.h>
#include <cartobase/object/pythonwriter.h>
#include <cartobase/stream/fileutil.h>
#include <cartobase/stream/directory.h>
#include <cartobase/uuid/uuid.h>
#include <qsqldatabase.h>
#include <qsqlquery.h>
#include <qsqlerror.h>
#include <qvariant.h>

using namespace aims;
using namespace carto;
using namespace std;


class QSqlGraphDatabase
{
public:
  typedef std::map<std::string, std::map<std::string,
    std::vector<std::string> > > Syntax;

  struct CurrentGraphData
  {
    CurrentGraphData( int eid, Graph* g ) : gid( eid ), graph( g ) {}

    int gid;
    Graph* graph;
    std::map<int, int> vertexindex2eid;
    std::map<Vertex *, int> vertexeid;
    std::map<int, int> edgeindex2eid;
    std::map<Edge *, int> edgeeid;
  };

  QSqlGraphDatabase();
  ~QSqlGraphDatabase();

  void setUrl( const std::string & url,
               carto::Object header = carto::none() );
  bool open();
  bool open( const std::string & user, const std::string & password );
  void close();
  QSqlQuery exec( const std::string & query ) const;
  QSqlError lastError() const;

  std::string hostname() const;
  QSqlDatabase & database() const;
  carto::rc_ptr<Syntax> attributesSyntax;
  carto::Object header();
  carto::Object parsedUrl();
  void setHeader( carto::Object hdr );

  bool readSchema();
  // bool hasGraph( int eid );
  // int findGraph( const std::string & uuid );
  // int findGraph( Graph & );
  // int findVertex( Vertex & );
  // int findEdge( Edge & );
  // int readGraphProperties( Graph & graph, int eid=-1 );
  // int readVertexProperties( Graph & graph, Vertex & vert, int eid=-1 );
  // int readEdgeProperties( Graph & graph, Edge & vert, int eid=-1 );
  void updateElement( const GraphObject & el, int eid );
  void sqlAttributes( const GraphObject & el,
                      std::map<std::string, std::string> & atts ) const;
  void insertOrUpdateVertex( Vertex* vert, CurrentGraphData & data );
  void insertOrUpdateEdge( Edge* v, CurrentGraphData & data );
  void fillGraphData( CurrentGraphData & data, Graph & g, int geid=-1 );
  void updateVertexIndexMap( CurrentGraphData & data );
  void updateEdgeIndexMap( CurrentGraphData & data );
  void updateVertexMap( CurrentGraphData & data );
  void updateEdgeMap( CurrentGraphData & data );

private:
  struct Private;
  Private *d;
  std::string _hostname;
  carto::Object _header;
  carto::Object _parsedurl;
  mutable QSqlDatabase *_database;
  std::string _connectionname;
};

// -----

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

  void completeAttributes(
    map<string, map<string, vector<string> > > & attributes,
    const string & classname, const map<string, list<string> > & inherits,
    set<string> & done )
  {
    if( done.find( classname ) != done.end() )
      return;
    done.insert( classname );
    map<string, list<string> >::const_iterator ih = inherits.find( classname );
    if( ih != inherits.end() )
    {
      const list<string> & bl = ih->second;
      list<string>::const_reverse_iterator il, el = bl.rend();
      map<string, vector<string> > & atts = attributes[ classname ];
      for( il=bl.rbegin(); il!=el; ++il )
      {
        completeAttributes( attributes, *il, inherits, done );
        map<string, vector<string> > & batts = attributes[ *il ];
        atts.insert( batts.begin(), batts.end() );
      }
    }
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
    float value = (float) res.toDouble( &ok );
    if( ok )
      return Object::value( value );
    return none();
  }


  Object doubleHelper( const QVariant & res, bool & ok )
  {
    double value = res.toDouble( &ok );
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
      semanticTypes[ "Double()" ] = doubleHelper;
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


  void attributesAsString( const GraphObject & graph,
                           map<string, vector<string> > & gatts, string & sql,
                           string & values, bool firstinlist = true )
  {
    map<string, vector<string> >::iterator is, es = gatts.end();
    Object it = graph.objectIterator();
    PythonWriter pw;
    stringstream sval;
    pw.attach( sval );
    pw.setSingleLineMode( true );
    for( is=gatts.begin(); is!=es; ++is )
    {
      Object val;
      string name = is->first;
      if( !is->second[ 1 ].empty() )
        name = is->second[ 1 ];
      try
      {
        val = graph.getProperty( name );
      }
      catch( ... )
      {
        if( is->second[ 2 ] == "needed" && name != "graph"
          && name != "vertex1" && name != "vertex2" )
          cout << "Missing mandatory attribute " << name << endl;
        continue;
      }
      if( firstinlist )
        firstinlist = false;
      else
      {
        sql += ", ";
        sval << ", ";
      }
      sql += is->first;
      const string & type = is->second[ 0 ];
      bool tostr = !( (type == "Int()") || (type == "Float()")
        || (type=="Double()") || (val->type() == "string") );
      if( tostr )
      {
        // transform into string
        stringstream sval2;
        pw.attach( sval2 );
        pw.write( val, false, false ); // print 1st as string
        // then make an Object from that string
        Object s2 = Object::value( sval2.str() );
        pw.attach( sval );
        // then re-write, correctly escaping characters
        pw.write( s2, false, false );
      }
      else
        pw.write( val, false, false );
    }
    values += sval.str();
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
  QSqlGraphDatabase db;
  Object hdr;
  bool hadhdr = graph.getProperty( "header", hdr );
  if( hadhdr )
    db.setHeader( hdr );
  db.setUrl( filename1, hdr );
  string syntax, filename = db.hostname();
  hdr = db.header();
  if( !hadhdr )
    static_cast<QSqlGraphFormatHeader *>( hdr.get() )->read();

  vector<int> gids;
  int gid = -1, targetgid = -1;
  vector<string> syntaxs;

  if( !hdr->getProperty( "graph_sql_eid", gids ) || gids.size() == 0 )
    throw wrong_format_error( "database does not contain any graph",
                              filename );

  if( gids.size() == 1 )
  {
    hdr->getProperty( "arg_syntax", syntax );
    syntaxs.push_back( syntax );
  }
  else
  {
    hdr->getProperty( "arg_syntax", syntaxs );
  }

  // parse URL and retreive request if any
  Object parsedurl = db.parsedUrl();
  string query, graphquery;
  parsedurl->getProperty( "query", query );
  // simple pre-parsing
  string::size_type t = query.find( "Graph.eid=" );
  if( t != string::npos )
  {
    istringstream sst( query.substr( t + 10, query.length() - t - 10 ) );
    sst >> targetgid;
  }
  else
  {
    t = query.find( "Graph." );
    if( t != string::npos )
    {
      string::size_type t2 = query.find( ';', t );
      if( t2 != string::npos )
        graphquery = query.substr( t2, t - t2 - 1 );
      else
        graphquery = query.substr( t, query.length() - t );
    }
  }

  if( targetgid >= 0 )
  {
    ostringstream osgid;
    osgid << targetgid;
    string sgid = osgid.str();
    unsigned i, n = gids.size();
    for( i=0; i<n; ++i )
      if( gids[i] == targetgid )
        break;
    if( i == n )
      throw invalid_format_error( "database does not contain the requested "
        "graph " + sgid, filename );
    gid = targetgid;
    if( syntaxs.size() <= i )
      throw invalid_format_error( "graphs IDs and types mismatch", filename );
    syntax = syntaxs[i];
  }
  else if( gids.size() == 1 )
  {
    gid = gids[0];
    syntax = syntaxs[0];
  }

  // (re-)open DB connection
  bool ok = db.open();
  if( !ok )
  {
    throw wrong_format_error( db.lastError().text().utf8().data(),
                              filename );
  }

  // find graph if a SQL query is provided
  if( !graphquery.empty() )
  {
    QSqlQuery res = db.exec( string( "SELECT eid FROM Graph WHERE " )
      + graphquery );
    if( res.lastError().type() != 0 )
      throw invalid_format_error( string( "Graph reader, graph selection:" )
        + res.lastError().text().utf8().data(), filename );
    set<int> selgids;
    while( res.next() )
      selgids.insert( res.value(0).toInt() );
    if( selgids.size() != 1 )
      throw invalid_format_error(
        "Graph reader, graph selection: did not select one graph", filename );
    gid = *selgids.begin();
    unsigned i, n = gids.size();
    for( i=0; i<n; ++i )
      if( gids[i] == gid )
        break;
    if( i == n )
      throw invalid_format_error( "database does not contain the requested "
        "graph", filename );
    if( syntaxs.size() <= i )
      throw invalid_format_error( "graphs IDs and types mismatch", filename );
    syntax = syntaxs[i];
  }
  if( gid < 0 )
    throw invalid_format_error( "database contains multiple graphs",
                                filename );
  ostringstream osgid;
  osgid << gid;
  string sgid = osgid.str();
  graph.setSyntax( syntax );

  // read syntax in DB
  db.readSchema();
  rc_ptr<map<string, map<string, vector<string> > > > syntaxattributes
    = db.attributesSyntax;
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
  QSqlQuery res = db.exec( sql );
  if( res.lastError().type() != 0 )
    throw wrong_format_error( res.lastError().text().utf8().data(),
                              filename );

  res.next();
  fillItem( &graph, res, gatts, 0 );

  // retreive vertices types
  sql = "SELECT class_name FROM class JOIN _Vertex ON _Vertex.eid=class.eid"
    " WHERE _Vertex.graph=" + sgid + " GROUP BY class_name";
  res = db.exec( sql );
  if( res.lastError().type() != 0 )
    throw wrong_format_error( res.lastError().text().utf8().data(),
                              filename );
  list<string> vtypes;
  while( res.next() )
    vtypes.push_back( res.value(0).toString().utf8().data() );
  map<int, Vertex *> id2vertex;

  // cout << "vertices types: " << vtypes.size() << endl;
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
    // cout << "vtype: " << vt << ", SQL:\n" << sql << endl;
    res = db.exec( sql );
    if( res.lastError().type() != 0 )
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
  res = db.exec( sql );
  if( res.lastError().type() != 0 )
    throw wrong_format_error( res.lastError().text().utf8().data(),
                              filename );
  list<string> etypes;
  while( res.next() )
    etypes.push_back( res.value(0).toString().utf8().data() );

  // cout << "edges types: " << etypes.size() << endl;
  // select edges
  evt = etypes.end();
  for( ivt=etypes.begin(); ivt!=evt; ++ivt )
  {
    const string & et = *ivt;
    // cout << "edge type: " << et << endl;
    const map<string, vector<string> > & eatts = attributes[ et ];
    sql = string( "SELECT " ) + et + ".vertex1, " + et + ".vertex2";
    map<string, vector<string> >::const_iterator ieat, eeat = eatts.end();
    for( ieat=eatts.begin(); ieat!=eeat; ++ieat )
      sql += string( ", " ) + et + "." + ieat->first;
    sql += " FROM " + et + " JOIN _Vertex ON _Vertex.eid=vertex1 WHERE "
      "_Vertex.graph=" + sgid;
    // cout << "etype: " << et << ", SQL:\n" << sql << endl;
    res = db.exec( sql );
    if( res.lastError().type() != 0 )
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


bool QSqlGraphFormat::write( const std::string & filename1,
                             const Graph & graph, bool )
{
  cout << "QSqlGraphFormat::write " << filename1 << endl;

  // WARNING we may require non-const access to the graph to update its
  // internal IO state
  Graph & nonconst_graph = const_cast<Graph &>( graph );

  // now SQL part
  QSqlGraphDatabase db;
  db.setUrl( filename1 );
  QSqlGraphFormatHeader h( filename1 );
  Object parsedurl = db.parsedUrl();
  string filename = db.hostname();
  cout << "filename: " << filename << endl;

  /* for now, erase the output file if it exists.
     This behaviour is consistent with any other IO routines,
     but is unusual and *dangerous* on a database, which may contain several
     graphs and any other data.
     TODO: fix it */
/*  string st = FileUtil::fileStat( filename );
  if( st.find( '+' ) != string::npos )
  {
    unlink( filename.c_str() );
  }
*/

  bool ok = db.open();
  if( !ok )
  {
    throw wrong_format_error( db.lastError().text().utf8().data(),
                              filename );
  }

  // check if DB already contains the graph schema
  string sql = "SELECT name FROM _soma_classes WHERE name='Graph'";
  QSqlQuery res = db.exec( sql );
  ok = false;
  if( res.lastError().type() == 0 )
    while( res.next() )
    {
      ok = true;
      break;
    }
  if( !ok )
  {
    // cout << "creating schema\n";
    // create schema using pre-recorded map<string, vector<string> > SQL code
    // generated by soma-databases
    // (soma.database.graph_schema python module)
    string schemapath = Path::singleton().syntax() + FileUtil::separator()
      + "graphschema.sql";
    // cout << "SQL schema file: " << schemapath << endl;
    ifstream f( schemapath.c_str() );
    if( !f )
      io_error::launchErrnoExcept( schemapath );
    // read schema line by line
    while( !f.eof() )
    {
      string line;
      bool multiline = false;
      do
      {
        char c = f.get();
        if( f.eof() )
          break;
        while( c != '\0' && c != '\n' )
        {
          if( c != '\r' )
            line += c;
          c = f.get();
        }
        if( !multiline && line.length() >= 12
          && line.substr( line.length() - 12, 12 ) == "FOR EACH ROW" )
          multiline = true;
        else if( multiline && line.length() >= 3
          && line.substr( line.length() - 3, 3 ) == "END" )
          multiline = false;
        if( multiline )
          line += '\n';
      }
      while( multiline && !f.eof() );
      // execute SQL line
      res = db.exec( line );
      if( res.lastError().type() != 0 )
        throw wrong_format_error( res.lastError().text().utf8().data(),
                                  filename );
    }
    // cout << "SQL graph schema created\n";
  }

  // retreive or generate UUID
  string guuid;
  bool existinggraph = false;
  if( !graph.getProperty( "uuid", guuid ) )
  {
    guuid = UUID().toString();
    nonconst_graph.setProperty( "uuid", guuid );
  }
  else
    existinggraph = true;

  // first write meshes, buckets, volumes etc

  string datadir = FileUtil::removeExtension( filename ) + ".data";
  Directory ddir( datadir );
  if( !ddir.isValid() )
    ddir.mkdir();
  datadir
    = FileUtil::basename( datadir ) + FileUtil::separator() + guuid;
  nonconst_graph.setProperty( "filename_base", datadir );
  AimsGraphWriter   gw( filename );
  AimsGraphWriter::SavingMode   sm = AimsGraphWriter::Keep;
  /* if( forceglobal )
  sm = AimsGraphWriter::Global; */
  gw.writeElements( nonconst_graph, sm, sm );
  //cout << "writeElements done\n";

  // nonconst_graph.setProperty( "name", "noname" );
  // nonconst_graph.setProperty( "side", "noside" );
  // get or generate a UUID
  int gid = -1;
  string sgid;

  if( existinggraph )
  {
    existinggraph = false;
    // check if there are any graph with the same uuid
    res = db.exec( string( "SELECT Graph.eid, class.class_name FROM Graph"
      " JOIN class ON class.eid=Graph.eid WHERE Graph.uuid='" )
      + guuid + "'" );
    if( res.lastError().type() == 0 )
    {
      if( res.next() )
      {
        gid = res.value(0).toInt();
        existinggraph = true;
        cout << "found the graph already in the DB: eid = " << gid << endl;

        stringstream ssgid;
        ssgid << gid;
        sgid = ssgid.str();
      }
    }
  }
  // if yes, erase it with all nodes ?

  // FIXME: for now erase all because we don't have uids to identify data

  /* FIXME: don't delete because we cannot delete views (Graph, Vertex...)
      but we must delete concrete instances (CorticalFoldArg, fold...)
  cout << "deleting graphs\n";
  sql = "DELETE FROM Graph";
  res = db.exec( sql.c_str() );
  if( !res.lastError().type() == 0 )
    throw invalid_format_error( res.lastError().text().utf8().data(),
                                filename );
  cout << "deleting vertices\n";
  sql = "DELETE FROM Vertex";
  res = db.exec( sql.c_str() );
  if( !res.lastError().type() == 0 )
    throw invalid_format_error( res.lastError().text().utf8().data(),
                                filename );
  cout << "deleting edges\n";
  sql = "DELETE * FROM Edge";
  res = db.exec( sql.c_str() );
  if( !res.lastError().type() == 0 )
    throw invalid_format_error( res.lastError().text().utf8().data(),
                                filename );
  */

  //(re-) read syntax in DB
  db.readSchema();
  rc_ptr<map<string, map<string, vector<string> > > > syntaxattributes
    = db.attributesSyntax;
  map<string, map<string, vector<string> > > & attributes = *syntaxattributes;

  // cout << "schema read\n";

  QSqlGraphDatabase::CurrentGraphData gdata;
  if( existinggraph )
    db.fillGraphData( gdata, nonconst_graph, gid );
  else
  {
    gdata.gid = gid;
    gdata.graph = &nonconst_graph;
  }

  // write graph
  if( existinggraph )
    db.updateElement( graph, gid );
  else
  {
    sql = "INSERT INTO " + graph.getSyntax() + " ( ";
    string values;
    map<string, vector<string> > & gatt = attributes[ graph.getSyntax() ];
    attributesAsString( graph, attributes[ "Graph" ], sql, values );
    attributesAsString( graph, gatt, sql, values, false );
    sql += string( " ) values ( " ) + values + " )";
    // cout << "graph SQL: " << sql << endl;
    res = db.exec( sql );
    if( !res.lastError().type() == 0 )
      throw invalid_format_error( res.lastError().text().utf8().data(),
                                  filename );

    // get graph id
    // cout << "querying graph eid\n";
    res = db.exec( "SELECT eid FROM Graph ORDER BY eid DESC LIMIT 1" );
    if( !res.lastError().type() == 0 )
      throw invalid_format_error( res.lastError().text().utf8().data(),
                                  filename );
    res.next();
    int gid = res.value(0).toInt();
    cout << "graph eid: " << gid << endl;

    gdata.gid = gid;
    stringstream ssgid;
    ssgid << gid;
    sgid = ssgid.str();
  }

  // write vertices

  Graph::const_iterator iv, ev = graph.end();
  for( iv=graph.begin(); iv!=ev; ++iv )
    db.insertOrUpdateVertex( *iv, gdata );

  db.updateVertexIndexMap( gdata );
  db.updateVertexMap( gdata );

  // cout << "vertices written\n";

  // write edges

  set<Edge *>::const_iterator ie, ee = graph.edges().end();
  cout << "saving edges: " << graph.edges().size() << endl;
  for( ie=graph.edges().begin(); ie!=ee; ++ie )
    db.insertOrUpdateEdge( *ie, gdata );

  db.close();

  return true;
}

// ---

struct QSqlGraphDatabase::Private
{
  Private();

#if QT_VERSION >= 0x040000
  QSqlDatabase db;
#endif
};

QSqlGraphDatabase::Private::Private()
{
}


QSqlGraphDatabase::QSqlGraphDatabase()
  : d( new QSqlGraphDatabase::Private ), _database( 0 )
{
#if QT_VERSION >= 0x040000
  _database = &d->db;
#endif
}


QSqlGraphDatabase::~QSqlGraphDatabase()
{
  close();
  delete d;
}


inline QSqlDatabase & QSqlGraphDatabase::database() const
{
  return *_database;
}


inline carto::Object QSqlGraphDatabase::header()
{
  return _header;
}


inline carto::Object QSqlGraphDatabase::parsedUrl()
{
  return _parsedurl;
}


inline std::string QSqlGraphDatabase::hostname() const
{
  return _hostname;
}


void QSqlGraphDatabase::setHeader( const Object hdr )
{
  _header = hdr;
}


void QSqlGraphDatabase::setUrl( const string & url, Object hdr )
{
  _header = hdr;
  QSqlGraphFormatHeader *h = 0;
  string syntax, sqldbtype, filename;
  if( !hdr.isNull() )
    h = dynamic_cast<QSqlGraphFormatHeader *>( hdr.get() );
  if( !h )
  {
    h = new QSqlGraphFormatHeader( url );
    hdr.reset( h );
  }
  _hostname = h->filename();
  _connectionname = _hostname; // for now
  // parse URL and retreive request if any
  _parsedurl = h->parseUrl();
  
}


bool QSqlGraphDatabase::open()
{
  string sqldbtype;
  if( _parsedurl )
    _parsedurl->getProperty( "sql_database_type", sqldbtype );

#if QT_VERSION >= 0x040000
  d->db = QSqlDatabase::addDatabase( sqldbtype.c_str(),
                                     _connectionname.c_str() );
#else
  _database = QSqlDatabase::addDatabase( sqldbtype.c_str(),
                                         _connectionname.c_str() );
#endif

  database().setDatabaseName( _hostname.c_str() );
  return database().open();
}


bool QSqlGraphDatabase::open( const string & user, const string & password )
{
  string sqldbtype;
  if( _parsedurl )
    _parsedurl->getProperty( "sql_database_type", sqldbtype );

#if QT_VERSION >= 0x040000
  d->db = QSqlDatabase::addDatabase( sqldbtype.c_str(),
                                     _connectionname.c_str() );
#else
  _database = QSqlDatabase::addDatabase( sqldbtype.c_str(),
                                         _connectionname.c_str() );
#endif

  database().setDatabaseName( _hostname.c_str() );
  return database().open( user.c_str(), password.c_str() );
}


inline void QSqlGraphDatabase::close()
{
#if QT_VERSION >= 0x040000
  if( _database->isOpen() )
    database().close();
  delete d;
  QSqlDatabase::removeDatabase( _connectionname.c_str() );
  d = new Private;
  _database = &d->db;
#else
  if( _database && _database->isOpen() )
    _database->close();
  if( _database )
  {
    QSqlDatabase::removeDatabase( _database );
    _database = 0;
  }
#endif
}


inline QSqlQuery QSqlGraphDatabase::exec( const std::string & query ) const
{
  return database().exec( query.c_str() );
}


inline QSqlError QSqlGraphDatabase::lastError() const
{
  return database().lastError();
}


bool QSqlGraphDatabase::readSchema()
{
  // read inheritance map
  QSqlQuery res = database().exec( "SELECT name, base FROM soma_classes" );
  if( database().lastError().type() != 0 )
    throw syntax_check_error( database().lastError().text().utf8().data(),
                              hostname() );
  map<string, string> bases;
  while( res.next() )
  {
    string base = res.value(1).toString().utf8().data();
    if( !base.empty() )
      bases[ res.value(0).toString().utf8().data() ] = base;
  }
  map<string, list<string> > inherits;
  map<string, list<string> >::iterator ihh, ehh = inherits.end();
  map<string, string>::iterator ih, eh = bases.end(), ih2;
  for( ih=bases.begin(); ih!=eh; ++ih )
  {
    list<string> & inhd = inherits[ih->first];
    inhd.push_back( ih->second );
    for( ih2=bases.find( ih->second ); ih2!=eh;
      ih2=bases.find( ih2->second ) )
    {
      inhd.push_back( ih2->second );
    }
  }

  res = database().exec( "SELECT soma_class, name, type, label, optional,"
    " default_value FROM soma_attributes" );
  if( database().lastError().type() != 0 )
    throw syntax_check_error( database().lastError().text().utf8().data(),
                              hostname() );
  typedef map<string, map<string, vector<string> > > ResType;
  ResType *attributes = new ResType;
  bool ok = false, optional;
  /* cout << "syntax:\n";
  cout << "class, name, type, label, optional, default_value\n"; */
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
    string classname = res.value(1).toString().utf8().data();
    vector<string> & sem = gt[ classname ];
    sem.reserve( 4 );
    sem.push_back( res.value(2).toString().utf8().data() );
    sem.push_back( res.value(3).toString().utf8().data() );
    sem.push_back( optional ? "" : "needed" );
    sem.push_back( res.value(5).toString().utf8().data() );
    /* cout << t << " | " << res.value(1).toString().utf8().data() << " | "
    << sem[0] << " | " << sem[1] << " | " << sem[2] << " | " << sem[3]
    << endl; */
  }


  // complete attributes with inherited ones
  ResType::iterator ia, ea = attributes->end();
  set<string> done;
  for( ia=attributes->begin(); ia!=ea; ++ia )
  {
    const string & classname = ia->first;
    ihh = inherits.find( classname );
    if( ihh != ehh )
    {
      list<string>::iterator il, el = ihh->second.end();
      completeAttributes( *attributes, classname, inherits, done );
    }
  }

  attributesSyntax.reset( attributes );
  return !attributesSyntax.isNull() && !attributesSyntax->empty();
}


void QSqlGraphDatabase::updateElement( const GraphObject & element, int eid )
{
  QString sql = QString( "UPDATE " ) + element.getSyntax().c_str()
    + " SET ";
  map<string, string> vals;
  sqlAttributes( element, vals );
  bool first = true;
  map<string, string>::iterator i, e = vals.end();
  for( i=vals.begin(); i!=e; ++i )
  {
    if( first )
      first = false;
    else
      sql += ", ";
    sql += QString( i->first.c_str() ) + "=" + i->second.c_str();
  }
  sql += QString( " WHERE eid=" ) + QString::number( eid );
  cout << "element update SQL: " << sql.utf8().data() << endl;
  QSqlQuery res = database().exec( sql );
  if( !res.lastError().type() == 0 )
  {
    cout << "SQL error: " << res.lastError().text().utf8().data() << endl;
    throw invalid_format_error( res.lastError().text().utf8().data(),
                                _hostname );
  }

}


void QSqlGraphDatabase::sqlAttributes( const GraphObject & element,
                                       map<string, string> & atts ) const
{
  Syntax::const_iterator ist = attributesSyntax->find( element.getSyntax() );
  if( ist == attributesSyntax->end() )
    return;

  const map<string, vector<string> > & satts = ist->second;
  map<string, vector<string> >::const_iterator is, es = satts.end();
  Object it = element.objectIterator();
  PythonWriter pw;
  pw.setSingleLineMode( true );
  for( is=satts.begin(); is!=es; ++is )
  {
    stringstream sval;
    pw.attach( sval );
    Object val;
    string name = is->first;
    if( !is->second[ 1 ].empty() )
      name = is->second[ 1 ];
    try
    {
      val = element.getProperty( name );
    }
    catch( ... )
    {
      if( is->second[ 2 ] == "needed" && name != "graph"
        && name != "vertex1" && name != "vertex2" )
        cout << "Missing mandatory attribute " << name << endl;
      continue;
    }
    const string & type = is->second[ 0 ];
    bool tostr = !( (type == "Int()") || (type == "Float()")
    || (type=="Double()") || (val->type() == "string") );
    if( tostr )
    {
      // transform into string
      stringstream sval2;
      pw.attach( sval2 );
      pw.write( val, false, false ); // print 1st as string
      // then make an Object from that string
      Object s2 = Object::value( sval2.str() );
      pw.attach( sval );
      // then re-write, correctly escaping characters
      pw.write( s2, false, false );
    }
    else
      pw.write( val, false, false );
    atts[ is->first ] = sval.str();
  }
}


void QSqlGraphDatabase::fillGraphData(
  QSqlGraphDatabase::CurrentGraphData & data, Graph & g, int eid )
{
  data.graph = &g;
  if( eid < 0 )
  {
    string uuid;
    g.getProperty( "uuid" );
    string sql = string( "SELECT eid FROM _Graph WHERE uuid='" ) + uuid + "'";
    QSqlQuery res = exec( sql );
    if( !res.lastError().type() == 0 )
      throw invalid_format_error( res.lastError().text().utf8().data(),
                                  _hostname );
    res.next();
    eid = res.value(0).toInt();
    data.gid = eid;
  }
  data.gid = eid;
  updateVertexIndexMap( data );
  updateEdgeIndexMap( data );
}


void QSqlGraphDatabase::insertOrUpdateVertex( Vertex* v,
                                              CurrentGraphData & data )
{
  int index = -1, eid = -1;
  if( !v->getProperty( "index", index ) )
  {
    if( data.vertexindex2eid.empty() )
      index = 0;
    else
      index = data.vertexindex2eid.rbegin()->first + 1;
    v->setProperty( "index", index );
  }
  else
  {
    map<int, int>::iterator ii = data.vertexindex2eid.find( index );
    if( ii != data.vertexindex2eid.end() )
      eid = ii->second;
  }
  if( eid < 0 )
  {
    // insert
    // cout << "insert vertex " << v << ": " << v->getSyntax() << endl;
    string sql = "INSERT INTO " + v->getSyntax() + " ( graph";
    string values = QString::number( data.gid ).utf8().data();
    map<string, vector<string> > & vatt = (*attributesSyntax)[ v->getSyntax() ];
    attributesAsString( *v, vatt, sql, values, false );
    sql += string( " ) values ( " ) + values + " )";
    // cout << "vertex SQL:\n" << sql << endl;
    QSqlQuery res = exec( sql );
    if( !res.lastError().type() == 0 )
      throw invalid_format_error( res.lastError().text().utf8().data(),
                                  hostname() );
    // get vertex id
/*    sql = string( "SELECT eid FROM " ) + v->getSyntax()
      + " ORDER BY eid DESC LIMIT 1";
    res = exec( sql );
    if( !res.lastError().type() == 0 )
      throw invalid_format_error( res.lastError().text().utf8().data(),
                                  hostname() );
    res.next();
    eid = res.value(0).toInt();*/
  }
  else
  {
    // update
    cout << "update existing vertex " << v << endl;
    updateElement( *v, eid );
  }
}


void QSqlGraphDatabase::insertOrUpdateEdge( Edge* v,
                                            CurrentGraphData & data )
{
  int index = -1, eid = -1;
  if( !v->getProperty( "index", index ) )
  {
    if( data.edgeindex2eid.empty() )
      index = 0;
    else
      index = data.edgeindex2eid.rbegin()->first + 1;
    v->setProperty( "index", index );
  }
  else
  {
    map<int, int>::iterator ii = data.edgeindex2eid.find( index );
    if( ii != data.edgeindex2eid.end() )
      eid = ii->second;
  }
  if( eid < 0 )
  {
    // insert
    // cout << "insert edge " << v << ": " << v->getSyntax() << endl;
    string sql = "INSERT INTO " + v->getSyntax() + " ( vertex1, vertex2";
    Edge::const_iterator ive = v->begin();
    stringstream vids;
    vids << data.vertexeid[ *ive ] << ", ";
    ++ive;
    vids << data.vertexeid[ *ive ];
    string values = vids.str();
    map<string, vector<string> > & eatt
      = (*attributesSyntax)[ v->getSyntax() ];
    attributesAsString( *v, eatt, sql, values, false );
    sql += string( " ) values ( " ) + values + " )";
    // cout << "Edge SQL: " << sql << endl;
    QSqlQuery res = exec( sql );
    if( !res.lastError().type() == 0 )
      throw invalid_format_error( res.lastError().text().utf8().data(),
                                  hostname() );
    // get edge id
/*    sql = string( "SELECT eid FROM " ) + v->getSyntax()
      + " WHERE eid>" + QString::number( previouseid ).utf8().data()
      + " ORDER BY eid DESC LIMIT 1";
    res = exec( sql );
    if( !res.lastError().type() == 0 )
      throw invalid_format_error( res.lastError().text().utf8().data(),
                                  hostname() );
    res.next();*/
    //eid = res.value(0).toInt();
  }
  else
  {
    // update
    cout << "update existing edge " << v << endl;
    updateElement( *v, eid );
  }
}


void QSqlGraphDatabase::updateVertexIndexMap( CurrentGraphData & data )
{
  QSqlQuery res
    = database().exec( "SELECT graph_index, eid FROM _Vertex WHERE graph="
      + QString::number( data.gid ) );
  if( !res.lastError().type() == 0 )
    throw invalid_format_error( res.lastError().text().utf8().data(),
                                hostname() );
  while( res.next() )
    data.vertexindex2eid[ res.value(0).toInt() ] = res.value(1).toInt();
}


void QSqlGraphDatabase::updateEdgeIndexMap( CurrentGraphData & data )
{
  QSqlQuery res
    = database().exec( "SELECT _Edge.graph_index, _Edge.eid FROM _Edge JOIN "
      "_Vertex ON _Vertex.eid=_Edge.vertex1 WHERE _Vertex.graph="
      + QString::number( data.gid ) );
  if( !res.lastError().type() == 0 )
    throw invalid_format_error( res.lastError().text().utf8().data(),
                                hostname() );
  while( res.next() )
    data.edgeindex2eid[ res.value(0).toInt() ] = res.value(1).toInt();
}


void QSqlGraphDatabase::updateVertexMap( CurrentGraphData & data )
{
  Graph::const_iterator iv, ev = data.graph->end();
  int index = -1;
  for( iv=data.graph->begin(); iv!=ev; ++iv )
  {
    if( (*iv)->getProperty( "index", index ) )
      data.vertexeid[ *iv ] = data.vertexindex2eid[ index ];
  }
}


void QSqlGraphDatabase::updateEdgeMap( CurrentGraphData & data )
{
  set<Edge *>::const_iterator iv, ev = data.graph->edges().end();
  int index = -1;
  for( iv=data.graph->edges().begin(); iv!=ev; ++iv )
  {
    if( (*iv)->getProperty( "index", index ) )
      data.edgeeid[ *iv ] = data.edgeindex2eid[ index ];
  }
}


