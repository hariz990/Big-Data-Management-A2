package buildIndex;

import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;

import spatialindex.quadtree.QuadTree;
import spatialindex.spatialindex.Point;
import spatialindex.spatialindex.Region;
import storage.invertedindex.FirstLevelInvertedIndex;
import storage.invertedindex.HilbertQgramTokenInvertedIndex;
import storage.invertedindex.InfrequentPositionalQgramInvertedIndex;
import storage.invertedindex.InfrequentQgramTokenInvertedIndex;
import storage.invertedindex.QgramTokenCountPairInvertedIndex;
import storage.invertedindex.SecondLevelInvertedIndex;
import storage.objectindex.SpatialObjectDatabase;
import unit.IDSet;
import unit.PositionalQgram;
import unit.QgramGenerator;
import unit.SpatialObject;

public class BuildAll
{

  public static void main( String[] args ) throws IOException
  {

    if ( args.length != 8 )
    {
      System.err.println( "Usage: BuildObjectQgramDatabase   " + 
        "0.datafile   " + "1.indexfile" 
          + "2.samllQgramlength   " + "3.largerQgramLength   " + "4.positionUpperBound"
          + "5.maxElement   " + "6.infrequent threshold   " + "7.sparse threshold" );
    }
    ///home/sheng/Desktop/Inspire2/sg.txt index 2 3 9 24 5 10
    
    String file = args[0];
    String indexFile = args[1];
    int qgramLength = Integer.parseInt( args[2] );
    int largerQgramLength = Integer.parseInt( args[3] );
    int positionUpperBound = Integer.parseInt( args[4] );
    int maxElement = Integer.parseInt( args[5] );
    int infrequentThreshold = Integer.parseInt( args[6] );
    int sparseThreshold = Integer.parseInt( args[7] );

    /**
     * build object database and quadtree
     */
    System.out.println( "building object database and Quadtree..." );


    LineNumberReader lineReader = new LineNumberReader( new FileReader( file ) );

    // spatial object database
    SpatialObjectDatabase objectDatabase =
        new SpatialObjectDatabase( indexFile, qgramLength, largerQgramLength, positionUpperBound );
    objectDatabase.create();
    //sg.txt
    QuadTree quadtree = new QuadTree( new Region( new double[] {0.0, 0.0}, new double[] {1.0, 1.0} ), maxElement );
//    QuadTree quadtree = new QuadTree( new Region( new double[] {82.41, 15.96}, new double[] {136.91, 54.573} ), maxElement );


    QgramGenerator smallQgramGen = new QgramGenerator( qgramLength );
    QgramGenerator largeQgramGen = new QgramGenerator( largerQgramLength );


    // create the infrequent q-gram inverted index
    InfrequentPositionalQgramInvertedIndex infrequentQgramIndex =
        new InfrequentPositionalQgramInvertedIndex( indexFile, qgramLength );
    infrequentQgramIndex.creatTree();

    // create the infrequent q-gram token inverted index
    InfrequentQgramTokenInvertedIndex infrequentQgramTokenIndex =
        new InfrequentQgramTokenInvertedIndex( indexFile );
    infrequentQgramTokenIndex.creatTree();

    // infrequent q-grams in memory
    HashSet< String > infrequentPositionalQgramSet = new HashSet< String >();
    HashSet< String > infrequentQgramTokenSet = new HashSet< String >();

    // save inverted database
    HashMap< String, IDSet > positionalQgramInvertedMap = new HashMap< String, IDSet >();
    HashMap< String, IDSet > qgramTokenInvertedMap = new HashMap< String, IDSet >();


    long startTime = System.currentTimeMillis();
    int objectid;
    String line = lineReader.readLine();
    String objectText;
    String[] temp;
    double lat, lng;
    while ( line != null )
    {
      temp = line.split( "\t" );
      objectid = Integer.parseInt( temp[0] );
      lat = Double.parseDouble( temp[1] );
      lng = Double.parseDouble( temp[2] );
      objectText = temp[3];
      
      if ( objectText.length() >= 3 )
      {                  
        // save to database
        SpatialObject spatialObject = new SpatialObject( lat, lng, objectText );
        objectDatabase.write( objectid, spatialObject );

        // insert to quadtree
        quadtree.insert( new Point( new double[] {lng, lat} ), objectid );

        // first m positional q-gram
        ArrayList< PositionalQgram > positionalQgramSet =
            smallQgramGen.getFirstMPositionalQgramArrayList( objectText, positionUpperBound );

        // q-grams for larger q value
        HashSet< String > QgramTokenSet = largeQgramGen.getQgramHashSet( objectText );


        // accumulate the count for positional q-gram
        for ( PositionalQgram gram : positionalQgramSet )
        {
          String posQgramString = gram.toString();
          IDSet idSet = positionalQgramInvertedMap.get( posQgramString );
          if ( idSet == null )
          {
            // store the object id to the positional qgram
            idSet = new IDSet( objectid );
            positionalQgramInvertedMap.put( posQgramString, idSet );
          }
          else
          {
            // store the object id to the positional qgram
            if ( idSet.size() < infrequentThreshold )
            {
              idSet.add( objectid );
            }
          }
        }

        // accumulate the count for q-gram
        if ( QgramTokenSet != null )
        {
          for ( String token : QgramTokenSet )
          {
            IDSet idSet = qgramTokenInvertedMap.get( token );
            if ( idSet == null )
            {
              idSet = new IDSet( objectid );
              qgramTokenInvertedMap.put( token, idSet );
            }
            else
            {
              if ( idSet.size() < infrequentThreshold )
              {
                idSet.add( objectid );
              }
            }
          }
        }
      }

      line = lineReader.readLine();
    }

    // commit object database
    objectDatabase.flush();
    // save the quadtree to file
    quadtree.save( indexFile );

    System.out.println( "object database and quad tree build time : "
        + (System.currentTimeMillis() - startTime) / 1000 + " second" );
    System.out.println( "number of sparse nodes in Quadtree: "
        + quadtree.getSparseNodeNumber( sparseThreshold ) );
    System.out.println( "number of leaf nodes in Quadtree: " + quadtree.getNumberOfLeaves() );
    System.out.println( "number of level in Quadtree: " + quadtree.getMaxDepth() );
    System.out.println();


   
    /**
     * save for the infrequent q-gram token
     */    
    startTime = System.currentTimeMillis();

    // if there are few objects in having the positional q-grams, save them in the infrequent q-gram
    // inverted index
    // store the positions of a q-gram in the first level inverted index;
    Iterator< Entry< String, IDSet > > itr = positionalQgramInvertedMap.entrySet().iterator();
    while ( itr.hasNext() )
    {
      Entry< String, IDSet > entry = itr.next();
      String gramString = entry.getKey();
      IDSet idSet = entry.getValue();

      // infrequent positional q-gram
      if ( idSet.size() < infrequentThreshold )
      {
        infrequentQgramIndex.put( gramString, idSet );
        infrequentPositionalQgramSet.add( gramString );
      }
    }

    Iterator< Entry< String, IDSet > > tokenItr = qgramTokenInvertedMap.entrySet().iterator();
    while ( tokenItr.hasNext() )
    {
      Entry< String, IDSet > entry = tokenItr.next();
      String token = entry.getKey();
      IDSet idSet = entry.getValue();

      // infrequent q-gram
      if ( idSet.size() < infrequentThreshold )
      {
        infrequentQgramTokenIndex.put( token, idSet );
        infrequentQgramTokenSet.add( token );
      }
    }


    System.out.println( "positional q-gram infrequent ratio : " + infrequentQgramIndex._map.size()
        + " / " + positionalQgramInvertedMap.size() );

    System.out.println( "q-gram token infrequent ratio : " + infrequentQgramTokenIndex._map.size()
        + " / " + qgramTokenInvertedMap.size() );


    // clear the memory version
    positionalQgramInvertedMap.clear();
    qgramTokenInvertedMap.clear();

    // save the infrequent q-gram inverted index
    infrequentQgramIndex.flush();
    infrequentQgramTokenIndex.flush();


    // print the index built time
    long totalTime = System.currentTimeMillis() - startTime;
    System.out.println( "infrequent inverted index building time : " + totalTime / 1000
        + " seconds" );
    System.out.println();


    indexBuilding(indexFile, qgramLength, largerQgramLength, positionUpperBound, maxElement,
			infrequentThreshold, sparseThreshold,  quadtree,
			infrequentPositionalQgramSet, infrequentQgramTokenSet,
			objectDatabase, infrequentQgramIndex,
			infrequentQgramTokenIndex);
  }

  
  /*
   * build index, implement this function
   */
  
  public static void  indexBuilding(String indexFile, int qgramLength, int largerQgramLength, int positionUpperBound, int maxElement,
			int infrequentThreshold, int sparseThreshold, QuadTree quadtree,
			HashSet<String> infrequentPositionalQgramSet, HashSet<String> infrequentQgramTokenSet,
			SpatialObjectDatabase objectDatabase, InfrequentPositionalQgramInvertedIndex infrequentQgramIndex,
			InfrequentQgramTokenInvertedIndex infrequentQgramTokenIndex) {
		FirstLevelInvertedIndex firstLevelInvertedIndex = new FirstLevelInvertedIndex(indexFile, qgramLength);
		firstLevelInvertedIndex.createTree();

				// create second level inverted index
				SecondLevelInvertedIndex secondLevelInvertedIndex = new SecondLevelInvertedIndex(indexFile, qgramLength);
				secondLevelInvertedIndex.createMap();

				QgramTokenCountPairInvertedIndex qgramTokenCountPairInvertedIndex = new QgramTokenCountPairInvertedIndex(
						indexFile);
				qgramTokenCountPairInvertedIndex.createTree();

				HilbertQgramTokenInvertedIndex hilbertQgramTokenInvertedIndex = new HilbertQgramTokenInvertedIndex(indexFile);
				hilbertQgramTokenInvertedIndex.createTree();

				// build inverted database
				Long startTime = System.currentTimeMillis();

				quadtree.buildInvertedIndexNew(firstLevelInvertedIndex, qgramTokenCountPairInvertedIndex,
						secondLevelInvertedIndex, hilbertQgramTokenInvertedIndex, objectDatabase, infrequentPositionalQgramSet,
						infrequentQgramTokenSet, sparseThreshold);

				// save inverted database
				firstLevelInvertedIndex.flush();
				secondLevelInvertedIndex.flush();
				qgramTokenCountPairInvertedIndex.flush();
				hilbertQgramTokenInvertedIndex.flush();

				// print the index built time
				Long totalTime = System.currentTimeMillis() - startTime;
				System.out.println("node-level and object-level inverted index build time: " + totalTime / 1000 + " seconds");
				System.out.println(" ");

				System.out.println("number of sparse nodes in Quadtree: " + quadtree.getSparseNodeNumber(sparseThreshold));
				System.out.println("number of leaf nodes in Quadtree: " + quadtree.getNumberOfLeaves());
				System.out.println("number of level in Quadtree: " + quadtree.getMaxDepth());
				System.out.println("infrequent positional q-gram inverted index size: " + infrequentQgramIndex._map.size());
				System.out.println("first-level positional q-gram inverted index size: " + firstLevelInvertedIndex._map.size());
				System.out.println("second-level positional q-gram inverted index size: " + secondLevelInvertedIndex._map.size());
				System.out.println("infrequent q-gram token inverted index size: " + infrequentQgramTokenIndex._map.size());
				System.out.println("first-level q-gram token inverted index size: " + qgramTokenCountPairInvertedIndex._map.size());
				System.out.println("second-level q-gram token inverted index size: " + hilbertQgramTokenInvertedIndex._map.size());
	}
}

