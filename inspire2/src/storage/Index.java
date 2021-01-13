package storage;

import spatialindex.quadtree.QuadTree;
import storage.invertedindex.FirstLevelInvertedIndex;
import storage.invertedindex.HilbertQgramTokenInvertedIndex;
import storage.invertedindex.InfrequentPositionalQgramInvertedIndex;
import storage.invertedindex.InfrequentQgramTokenInvertedIndex;
import storage.invertedindex.QgramTokenCountPairInvertedIndex;
import storage.invertedindex.SecondLevelInvertedIndex;
import storage.objectindex.SpatialObjectDatabase;


public class Index
{
  public QuadTree quadtree;

  public SpatialObjectDatabase objectDatabase;

  // positional q-gram inverted index
  public FirstLevelInvertedIndex firstLevelInvertedIndex;

  public SecondLevelInvertedIndex secondLevelInvertedIndex;

  public InfrequentPositionalQgramInvertedIndex infrequentPosQgramInvertedIndex;

  // q-gram token inverted index
  public InfrequentQgramTokenInvertedIndex infrequentTokenInvertedIndex;

  public QgramTokenCountPairInvertedIndex qgramTokenCountPairInvertedIndex;

  public HilbertQgramTokenInvertedIndex hilbertQgramTokenInvertedIndex;
  
  private int smallQValue;
  private int largeQValue;
  private int positionUpperBound;
  
  
  
  public Index ( String dataFile, 
                  String infrequentIndexFile,
                  String invertedIndexFile,  
                  int smallQValue,
                  int largeQValue,
                  int positionUpperBound )
  {
    this.smallQValue = smallQValue;
    this.largeQValue = largeQValue;
    this.positionUpperBound = positionUpperBound;
    
    
    // load spatial object database
    SpatialObjectDatabase objectDatabase =  new SpatialObjectDatabase( dataFile, smallQValue, largeQValue, positionUpperBound );
    objectDatabase.load();

    // load quad tree
    System.out.println( "loading quad tree from file" );
    QuadTree quadTree = new QuadTree();
    quadTree = quadTree.load( invertedIndexFile );
    
    
    // load infrequent inverted database
    InfrequentPositionalQgramInvertedIndex infrequentInvertedIndex =  new InfrequentPositionalQgramInvertedIndex( infrequentIndexFile, smallQValue );
    infrequentInvertedIndex.loadTree();

    InfrequentQgramTokenInvertedIndex infrequentTokenInvertedIndex =  new InfrequentQgramTokenInvertedIndex( infrequentIndexFile );
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
    
  }
  
  
  
  
  
}
