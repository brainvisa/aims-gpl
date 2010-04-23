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

#ifndef AIMS_IO_QSQLGRAPHMANIP_H
#define AIMS_IO_QSQLGRAPHMANIP_H

#include <graph/graph/graph.h>
#include <qsqldatabase.h>
#include <qsqlquery.h>
#include <qsqlerror.h>


namespace aims
{

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
                        std::map<std::string, std::string> & atts,
                        bool addinherited = true,
                        const std::string & assyntax = std::string() ) const;
    void sqlAttributesAsStrings( const GraphObject & el, std::string & names,
                                std::string & values, bool first = true,
                                bool addinherited = true ) const;
    void insertOrUpdateVertex( Vertex* vert, CurrentGraphData & data );
    void insertOrUpdateEdge( Edge* v, CurrentGraphData & data );
    void fillGraphData( CurrentGraphData & data, Graph & g, int geid=-1 );
    void updateVertexIndexMap( CurrentGraphData & data );
    void updateEdgeIndexMap( CurrentGraphData & data );
    void updateVertexMap( CurrentGraphData & data );
    void updateEdgeMap( CurrentGraphData & data );

    carto::rc_ptr<Syntax> attributesSyntax;
    std::map<std::string, std::list<std::string> > syntaxInheritance;

  private:
    struct Private;
    Private *d;
    std::string _hostname;
    carto::Object _header;
    carto::Object _parsedurl;
    mutable QSqlDatabase *_database;
    std::string _connectionname;
  };


  // ------

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


  inline QSqlQuery QSqlGraphDatabase::exec( const std::string & query ) const
  {
    return database().exec( query.c_str() );
  }


  inline QSqlError QSqlGraphDatabase::lastError() const
  {
    return database().lastError();
  }

}

#endif

