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

#include <aims/graph/qsqlgraphformatheader.h>
#include <aims/graph/qsqlgraphdatabase.h>
#include <aims/io/finder.h>
#include <aims/io/aimsGraphR.h>
#include <aims/io/aimsGraphW.h>
#include <aims/def/path.h>
#include <graph/graph/graph.h>
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

}

// -------


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


void QSqlGraphDatabase::close()
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

  syntaxInheritance.clear();
  map<string, list<string> >::iterator ihh, ehh = syntaxInheritance.end();
  map<string, string>::iterator ih, eh = bases.end(), ih2;
  for( ih=bases.begin(); ih!=eh; ++ih )
  {
    list<string> & inhd = syntaxInheritance[ih->first];
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


/*
  // complete attributes with inherited ones
  ResType::iterator ia, ea = attributes->end();
  set<string> done;
  for( ia=attributes->begin(); ia!=ea; ++ia )
  {
    const string & classname = ia->first;
    ihh = syntaxInheritance.find( classname );
    if( ihh != ehh )
    {
      list<string>::iterator il, el = ihh->second.end();
      completeAttributes( *attributes, classname, syntaxInheritance, done );
    }
  }
  */

  attributesSyntax.reset( attributes );
  return !attributesSyntax.isNull() && !attributesSyntax->empty();
}


void QSqlGraphDatabase::updateElement( const GraphObject & element, int eid )
{
  // for now updating views is not supported in the schema, so
  // we fill individual _fold, _Vertex tables with inheritances
  list<string> ltables;
  ltables.push_back( element.getSyntax() );
  map<string, list<string> >::const_iterator
    ih = syntaxInheritance.find( element.getSyntax() );
  if( ih != syntaxInheritance.end() )
    ltables.insert( ltables.end(), ih->second.begin(), ih->second.end() );

  // for each table
  list<string>::const_iterator is, es = ltables.end();
  for( is=ltables.begin(); is!=es; ++is )
  {
    QString sql = QString( "UPDATE _" ) + is->c_str()
      + " SET ";
    map<string, string> vals;
    sqlAttributes( element, vals, false, *is );
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
    if( res.lastError().type() != 0 )
    {
      cout << "SQL error: " << res.lastError().text().utf8().data() << endl;
      throw invalid_format_error( res.lastError().text().utf8().data(),
                                  _hostname );
    }
  }

  // TODO: what for attributes which are not listed (deleted) ?

}


void QSqlGraphDatabase::sqlAttributes( const GraphObject & element,
                                       map<string, string> & atts,
                                       bool addinherited,
                                       const string & assyntax ) const
{
  string syntax = assyntax.empty() ? element.getSyntax() : assyntax;
  if( addinherited )
  {
    map<string, list<string> >::const_iterator
      ih = syntaxInheritance.find( syntax );
    if( ih != syntaxInheritance.end() )
    {
      list<string>::const_reverse_iterator is, es = ih->second.rend();
      for( is=ih->second.rbegin(); is!=es; ++is )
        sqlAttributes( element, atts, false, *is );
    }
  }

  Syntax::const_iterator ist = attributesSyntax->find( syntax );
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


void QSqlGraphDatabase::sqlAttributesAsStrings( const GraphObject & element,
                                                string & names,
                                                string & values, bool first,
                                                bool addinherited ) const
{
  map<string, string> atts;
  sqlAttributes( element, atts, addinherited );
  if( atts.empty() )
    return;
  map<string, string>::iterator ia, ea = atts.end();
  for( ia=atts.begin(); ia!=ea; ++ia )
  {
    if( first )
      first = false;
    else
    {
      names += ", ";
      values += ", ";
    }
    names += ia->first;
    values += ia->second;
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
    data.vertexindex2eid[ index ] = -1;
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
    sqlAttributesAsStrings( *v, sql, values, false );
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
    data.edgeindex2eid[ index ] = -1;
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
    sqlAttributesAsStrings( *v, sql, values, false );
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


namespace
{

  list<string> parseSelectionQuery( const string & sql,
                                    const string & filename )
  {
    if( sql.substr( 0, 7 ) != "SELECT " )
      throw invalid_format_error( "SQL query doesn't start by SELECT",
                                  filename );
    string::size_type pos = 7, n = sql.length();
    list<string> attributes;
    string current;
    char c;
    bool sep = true;
    for( ; pos!=n; ++pos )
    {
      c = sql[pos];
      if( c == ' ' || c == '\t' )
      {
        if( current.empty() )
          continue;
        else
        {
          attributes.push_back( current );
          current.clear();
          sep = false;
        }
      }
      else if( c == ',' )
      {
        if( current.empty() )
        {
          if( sep )
            throw invalid_format_error( "spurious coma", filename );
          else
            sep = true;
        }
        else
        {
          attributes.push_back( current );
          current.clear();
          sep = true;
        }
      }
      else
      {
        if( current.empty() && !sep )
          break;
        current += c;
      }
    }
    if( !current.empty() )
      attributes.push_back( current );

    // filter out table names
    list<string>::iterator is, es = attributes.end();
    for( is=attributes.begin(); is!=es; ++is )
    {
      pos = is->find( '.' );
      if( pos != string::npos )
        is->erase( 0, pos+1 );
    }
    return attributes;
  }

}


list<Graph *>
QSqlGraphDatabase::partialReadFromVertexQuery( const string & sqlquery,
                                               list<CurrentGraphData> &
                                               graphsinfo,
                                               bool allownewgraphs )
{
  map<int, CurrentGraphData*> graphmap;
  list<CurrentGraphData>::iterator ig, eg = graphsinfo.end();
  for( ig=graphsinfo.begin(); ig!=eg; ++ig )
    graphmap[ig->gid] = &*ig;

  list<string> atts = parseSelectionQuery( sqlquery, hostname() );
  list<string>::iterator ia, ea = atts.end();
  int eidindex = -1, graphindex = -1, i;
  for( ia=atts.begin(), i=0; ia!=ea; ++ia, ++i )
  {
    cout << *ia << ", ";
    if( *ia == "eid" )
      eidindex = i;
    else if( *ia == "graph" )
      graphindex = i;
  }
  cout << endl;
  if( eidindex < 0 || graphindex < 0 )
    throw invalid_format_error(
      "SELECT request on vertices must query eid and graph", hostname() );

  QSqlQuery res = exec( sqlquery );
  if( !res.lastError().type() == 0 )
    throw invalid_format_error( res.lastError().text().utf8().data(),
                                hostname() );

  map<int, CurrentGraphData*>::iterator im, em = graphmap.end();
  list<Graph *> newgraphs;

  while( res.next() )
  {
    int eid = res.value( eidindex ).toInt();
    int graph = res.value( graphindex ).toInt();
    cout << "eid: " << eid << ", graph: " << graph << endl;
    im = graphmap.find( graph );
    if( im == em )
    {
      if( allownewgraphs )
      {
        Graph *g = new Graph;
        newgraphs.push_back( g );
        graphsinfo.push_back( CurrentGraphData( graph, g ) );
        graphmap[ graph ] = &*graphsinfo.rbegin();
        // TODO: query/ read graph syntax / attributes
        fillGraphData( *graphsinfo.rbegin(), *g, graph );
      }
      else
        continue;
    }

    // TODO: to be continued...
  }

  return newgraphs;
}

