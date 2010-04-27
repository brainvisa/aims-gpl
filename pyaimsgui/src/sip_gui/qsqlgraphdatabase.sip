
namespace aims
{

  class QSqlGraphDatabase
  {
%TypeHeaderCode
#include <aims/graph/qsqlgraphdatabase.h>
%End

  public:
    typedef std::map<std::string, std::map<std::string,
      std::vector<std::string> > > Syntax;

    struct CurrentGraphData
    {
      CurrentGraphData( int eid, Graph* g );

      int gid;
      Graph* graph;
      /*
      map_S32_S32 vertexindex2eid;
      std::map<Vertex *, int> vertexeid;
      std::map<int, int> edgeindex2eid;
      std::map<Edge *, int> edgeeid;
      */
    };

    QSqlGraphDatabase();
    ~QSqlGraphDatabase();

    void setUrl( const std::string & url,
                 carto::Object header = carto::none() );
    bool open();
    bool open( const std::string & user, const std::string & password );
    void close();
    QSqlQuery exec( const std::string & query ) const /PyName=exec_/;
    QSqlError lastError() const;

    std::string hostname() const;
    QSqlDatabase & database() const;
    carto::Object header();
    carto::Object parsedUrl();
    void setHeader( carto::Object hdr );

    bool readSchema();
    /* void sqlAttributes( const GraphObject & el,
                        map_STRING_STRING & atts,
                        bool addinherited = true,
                        const std::string & assyntax = std::string() ) const; */
    void sqlAttributesAsStrings( const GraphObject & el, std::string & names,
                                 std::string & values, bool first = true,
                                 bool addinherited = true ) const;
    void insertOrUpdateVertex( Vertex* vert,
                               aims::QSqlGraphDatabase::CurrentGraphData & data );
    void insertOrUpdateEdge( Edge* v,
                             aims::QSqlGraphDatabase::CurrentGraphData & data );
    list_rc_ptr_Graph
    partialReadFromVertexQuery( const std::string & sqlquery,
                                list_CurrentGraphData & graphsinfo,
                                bool allownewgraphs = true );

    void updateElement( const GraphObject & el, int eid );
    void fillGraphData( aims::QSqlGraphDatabase::CurrentGraphData & data,
                        Graph & g, int geid=-1 );
    void updateVertexIndexMap( aims::QSqlGraphDatabase::CurrentGraphData & data );
    void updateEdgeIndexMap( aims::QSqlGraphDatabase::CurrentGraphData & data );
    void updateVertexMap( aims::QSqlGraphDatabase::CurrentGraphData & data );
    void updateEdgeMap( aims::QSqlGraphDatabase::CurrentGraphData & data );

    // rc_ptr_Syntax attributesSyntax;
    // map_STRING_list_STRING syntaxInheritance;

  private:
  };


};

