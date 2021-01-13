package query;

import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.util.HashMap;
import java.util.AbstractMap.SimpleEntry;

import engine.QueryEngine;

import spatialindex.quadtree.QuadTree;
import spatialindex.spatialindex.Region;
import storage.invertedindex.FirstLevelInvertedIndex;
import storage.invertedindex.HilbertQgramTokenInvertedIndex;
import storage.invertedindex.InfrequentPositionalQgramInvertedIndex;
import storage.invertedindex.InfrequentQgramTokenInvertedIndex;
import storage.invertedindex.QgramTokenCountPairInvertedIndex;
import storage.invertedindex.SecondLevelInvertedIndex;
import storage.objectindex.SpatialObjectDatabase;
import unit.SpatialObject;
import unit.StopWatch;
import unit.query.QueryType;
import unit.query.SpatialQuery;


public class RunQueryLatest
{

  public static void main( String[] args ) throws IOException
  {
    // TODO Auto-generated method stub
    if ( args.length != 15 )
    {
      System.err.println( "Usage: RunQuery  0.datafile,  1.queryFile,  2.infrequentIndex,  3.invertedIndex,  " +
            " 4.smallQValue,   5.largeQValue,   6.positionUpperBound,  " +
            " 7.sparseThreshold,   8.selectivityThreshold,   9.resultSizeThreshold,   " +
            " 10.visitingNodeSizeThreshold,   11.scarceThreshold,   " +
            " 12.queryRangeRadius   13.areaEnlargedRatio  14.queryType");
      
	// index query.txt index index 2 3 9 10 1 1 24 0.01 0.0025 2 0

      return;
    }

    String dataFile = args[0];
    String queryFile = args[1];
    String infrequentFile = args[2];
    String invertedIndexFile = args[3];
    
    int smallQValue = Integer.parseInt( args[4] );
    int largeQValue = Integer.parseInt( args[5] );
    int positionUpperBound = Integer.parseInt( args[6] );       
    
    int sparseThreshold = Integer.parseInt( args[7] );
    int selectivityThreshold = Integer.parseInt( args[8] );
    int resultSizeThreshold = Integer.parseInt( args[9] );
    int visitingNodeSizeThreshold = Integer.parseInt( args[10] );
  
    
    double scarceThreshold = Double.parseDouble( args[11] );
    double rangeRadius = Double.parseDouble( args[12] );
    double areaEnlargedRatio = Double.parseDouble( args[13] );
    int queryType = Integer.parseInt( args[14] );

       
           
    System.out.println( "loading indexes" );
    System.out.println( dataFile );

   // load spatial object database
   SpatialObjectDatabase objectDatabase =  new SpatialObjectDatabase( dataFile, smallQValue, largeQValue, positionUpperBound );
   objectDatabase.load();

   // load quad tree
   System.out.println( "loading quad tree from file" );
   QuadTree quadTree = new QuadTree();
   quadTree = quadTree.load( invertedIndexFile );
   
   
   // load infrequent inverted database
   InfrequentPositionalQgramInvertedIndex infrequentInvertedIndex =  new InfrequentPositionalQgramInvertedIndex( infrequentFile, smallQValue );
   infrequentInvertedIndex.loadTree();

   InfrequentQgramTokenInvertedIndex infrequentTokenInvertedIndex =  new InfrequentQgramTokenInvertedIndex( infrequentFile );
   infrequentTokenInvertedIndex.loadTree();


   // load inverted indexes
   FirstLevelInvertedIndex firstLevelInvertedIndex =  new FirstLevelInvertedIndex( invertedIndexFile, smallQValue );
   firstLevelInvertedIndex.loadTree();

   SecondLevelInvertedIndex secondLevelInvertedIndex =  new SecondLevelInvertedIndex( invertedIndexFile, smallQValue );
   secondLevelInvertedIndex.loadMap();
   
   QgramTokenCountPairInvertedIndex qgramTokenCountPairInvertedIndex =  new QgramTokenCountPairInvertedIndex( invertedIndexFile );
   qgramTokenCountPairInvertedIndex.loadTree();

   HilbertQgramTokenInvertedIndex hilbertQgramTokenInvertedIndex = new HilbertQgramTokenInvertedIndex( invertedIndexFile );
   hilbertQgramTokenInvertedIndex.loadTree();

   QueryEngine engine = new QueryEngine( 
     quadTree, objectDatabase, 
     firstLevelInvertedIndex, secondLevelInvertedIndex, 
     infrequentInvertedIndex, infrequentTokenInvertedIndex, 
     qgramTokenCountPairInvertedIndex, hilbertQgramTokenInvertedIndex,
     sparseThreshold, 
     selectivityThreshold, resultSizeThreshold,
     visitingNodeSizeThreshold, 
     scarceThreshold, areaEnlargedRatio, positionUpperBound);

   StopWatch stopWatch = new StopWatch();
   
   LineNumberReader queryReader = new LineNumberReader(new FileReader(queryFile));

   String queryLine = queryReader.readLine();
   System.out.println(queryLine);
   String[] block = null;
   double lat; 
   double lng;
   String word;
   int count = 1;
   
   while( queryLine != null )
   {     
     // initialize a query given a point and a query range
     block = queryLine.split( "\t" );
     int id = Integer.parseInt( block[0] );
     lat = Double.parseDouble( block[1] );
     lng = Double.parseDouble( block[2] );
     word = block[3];
          
     double[] lowCood = new double [2];     
     lowCood[0] = lng - rangeRadius;
     lowCood[1] = lat - rangeRadius;
     
     double[] highCood = new double [2];     
     highCood[0] = lng + rangeRadius;
     highCood[1] = lat + rangeRadius;
          
     Region queryRegion = new Region( lowCood, highCood );     
     
     
     SpatialQuery sq = new SpatialQuery( QueryType.PREFIX_RANGE, word, queryRegion, smallQValue, largeQValue, positionUpperBound );
     sq.id = id;          
     HashMap< Integer, SimpleEntry < QueryType, SpatialObject > > resultMap;
     switch( queryType )
     {
       // query all
       case 0:
    	 resultMap = engine.query( sq, stopWatch );
         break;
         
       default:
    	 resultMap = engine.query( sq, stopWatch );
     }
     System.out.println("result " + resultMap.size() ); 
     System.out.println(resultMap.keySet());
     queryLine = queryReader.readLine();
     if (queryLine != null)
    	 	System.out.println(queryLine);
     
     if ( count % 500 == 0 )
     {
       System.out.println("processing " + count + " / 1000" ); 
     }
     count ++;
   }
   
   queryReader.close();
   System.out.println(stopWatch);         
  }
}
