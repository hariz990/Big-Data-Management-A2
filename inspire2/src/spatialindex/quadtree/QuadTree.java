package spatialindex.quadtree;



import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentNavigableMap;


import spatialindex.spatialindex.Point;
import spatialindex.spatialindex.Region;
import storage.invertedindex.FirstLevelInvertedIndex;
import storage.invertedindex.HilbertQgramTokenInvertedIndex;
import storage.invertedindex.InfrequentPositionalQgramInvertedIndex;
import storage.invertedindex.OneGramInvertedIndex;
import storage.invertedindex.QgramTokenCountPairInvertedIndex;
import storage.invertedindex.SecondLevelInvertedIndex;
import storage.objectindex.SpatialObjectDatabase;
import unit.BooleanObject;
import unit.HilbertCountMap;
import unit.IDSet;
import unit.IntersectingNodeStatsMap;
import unit.NodeStatistic;
import unit.PositionalQgram;
import unit.SecondLevelKey;
import unit.SpatialObject;
import unit.comparator.HilbertComparator;
import unit.comparator.HilbertComparatorByDistance;
import unit.comparator.PositionalQgramComparator;
import unit.comparator.SecondLevelKeyComparator;



public class QuadTree implements Serializable {
	
	private static final long serialVersionUID = 1L;
	public QuadTreeNode _rootNode;
	public Region _region;
	
	/**
	 * Default value for amount of elements
	 */
	protected final static int MAX_ELEMENTS = 1000;
	public int _maxElements;

	public QuadTree() {
		
	}

	public QuadTree(Region region) {
		this( region, MAX_ELEMENTS );
	}
	
	public QuadTree(Region region, int maxElements) {
		this._region = region;
		this._maxElements = maxElements;
		this._rootNode = new QuadTreeNode ( this._region, 0, _maxElements );
		
		double[] b = new double[] { this._region.m_pHigh[0], this._region.m_pLow[1] };
		double[] d = new double[] { this._region.m_pLow[0], this._region.m_pHigh[1] };		
		this._rootNode.setHibertCoor(this._region.m_pLow, b, this._region.m_pHigh, d);

		//		this._rootNode._hilbertCode = "0";
		this._rootNode._hilbertCode = "";
	}

	/**
	 * Add a new element to the QuadTree
	 * 
	 * @param point
	 * @param element
	 */
	public void insert (Point point, int objectid) {
		// if the _region contains the point
		if ( _region.contains(point) )
		{
			this._rootNode.insert( new QuadTreeObject( point, objectid) );
		}
	}
	
	/**
	 *
	 * @param region
	 * @return the hilbert coding for the intersecting nodes
	 */
	public ArrayList<String> getIntersectingNodeCoding(Region region)
	{
		if ( ! this._region.intersects(region) ) {
			return null;
	 	}
		
		ArrayList<String> result = new ArrayList<String>();
		this._rootNode.getIntersectingNodeCoding(region, result);
		return result;
	}

	
	public ArrayList<QuadTreeNode> getIntersectingLeafNode(Region region)
	{
		if ( ! this._region.intersects(region) ) {
			return null;
		}
		
		ArrayList<QuadTreeNode> result = new ArrayList<QuadTreeNode>();
		this._rootNode.getIntersectingLeafNode(region, result);
		return result;
	}
	
	
	/**
	 * Returns the startCoordinates
	 * 
	 * @return
	 */
	public Region getRegion() {
		return this._region;
	}

	/**
	 * Clear the QuadTree
	 */
	public void clear()
	{
		this._rootNode.clear();
	}

	/**
	 * Return the root node of this quad tree
	 * 
	 * @return
	 */
	public QuadTreeNode getRootNode()
	{
		return this._rootNode;
	}
	
	
	public void browse()
	{
		this._rootNode.browse();
	}
	
	public void save(String path)
	{
		ObjectOutputStream objectStream;
		try {
			objectStream = new ObjectOutputStream(new FileOutputStream(path + ".quadtree"));
			objectStream.writeObject(this);   
			objectStream.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}  
	}
	

	public QuadTree load(String path)
	{
		ObjectInputStream objectStream;
		try {
			objectStream = new ObjectInputStream(new FileInputStream(path + ".quadtree"));
			QuadTree tree = (QuadTree) objectStream.readObject();   
			objectStream.close();
			
			return tree;
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		
		return null;  
	}
	
	public int getNumberOfLeaves()
	{
		return this._rootNode.getNumberOfLeaves();
	}
	
	public int getSparseNodeNumber(int sparseThreshold)
	{
		return this._rootNode.getSparseNodeNumber(sparseThreshold);		
	}
	
	public int getMaxDepth()
	{
		return this._rootNode.getMaxDepth();
	}
	
	public void buildInvertedIndex (
			FirstLevelInvertedIndex firstLevelInvertedIndex,
			SecondLevelInvertedIndex secondLevelInvertedIndex,
			SpatialObjectDatabase objectDatabase,
			HashSet<String> infrequentQgramSet,			
			final int underLoadThreshold,
			final int qgramLength) 
	{		
		
		HashMap<String, HilbertCountMap> gramLocationInvertedMap = 
			new HashMap<String, HilbertCountMap>(); 
			
		//	build the inverted index
		this._rootNode.buildInvertedIndex(
				gramLocationInvertedMap,
				secondLevelInvertedIndex,
				objectDatabase,
				infrequentQgramSet,
				underLoadThreshold,
				qgramLength);		

				
		// store the inveretedMap in to the disk;
		Iterator< Entry< String, HilbertCountMap > > itr = gramLocationInvertedMap.entrySet().iterator(); 
		while ( itr.hasNext() )
		{
			Entry< String, HilbertCountMap > entry = itr.next();
			String positionalQgram = entry.getKey();
			HilbertCountMap locationMap = entry.getValue();	
			
			// save in the firstLevelInvertedIndex
			firstLevelInvertedIndex.write(positionalQgram, locationMap);
			
			itr.remove();			
		}
	}
	
	
	public void buildInvertedIndex (
			FirstLevelInvertedIndex firstLevelInvertedIndex,
			OneGramInvertedIndex oneGramInvertedIndex,
			SecondLevelInvertedIndex secondLevelInvertedIndex,
			SpatialObjectDatabase objectDatabase,
			HashSet<String> infrequentQgramSet,			
			final int underLoadThreshold,
			final int qgramLength) 
	{				
		HashMap<String, HilbertCountMap> gramLocationInvertedMap = 
			new HashMap<String, HilbertCountMap>(); 
		
		HashMap<String, HilbertCountMap> oneGramInvertedMap = 
			new HashMap<String, HilbertCountMap>(); 
			
		//	build the inverted index
		this._rootNode.buildInvertedIndex(
				gramLocationInvertedMap,
				oneGramInvertedMap,
				secondLevelInvertedIndex,
				objectDatabase,
				infrequentQgramSet,
				underLoadThreshold,
				qgramLength);		

				
		// store the inveretedMap in to the disk;
		Iterator< Entry< String, HilbertCountMap > > itr = gramLocationInvertedMap.entrySet().iterator(); 
		while ( itr.hasNext() )
		{
			Entry< String, HilbertCountMap > entry = itr.next();
			String positionalQgram = entry.getKey();
			HilbertCountMap locationMap = entry.getValue();	
			
			// save in the firstLevelInvertedIndex
			firstLevelInvertedIndex.write(positionalQgram, locationMap);
			
			itr.remove();			
		}
		
		itr = oneGramInvertedMap.entrySet().iterator(); 
		while ( itr.hasNext() )
		{
			Entry< String, HilbertCountMap > entry = itr.next();
			String positionalQgram = entry.getKey();
			HilbertCountMap locationMap = entry.getValue();	
			
			// save in the firstLevelInvertedIndex
			oneGramInvertedIndex.write(positionalQgram, locationMap);
			
			itr.remove();			
		}
	}
	
		
	public void buildInvertedIndexNew (
			FirstLevelInvertedIndex firstLevelInvertedIndex,
			QgramTokenCountPairInvertedIndex qgramTokenCountPairInvertedIndex,			
			SecondLevelInvertedIndex secondLevelInvertedIndex,
			HilbertQgramTokenInvertedIndex hilbertQgramTokenInvertedIndex,
			SpatialObjectDatabase objectDatabase,
			HashSet<String> infrequentPositionalQgramSet,	
			HashSet<String> infrequentQgramTokenSet,
			int sparseThreshold) 
	{					  		  	  
		Map<String, Map<String, Integer>> gramLocationInvertedMap = 
			new HashMap<String, Map<String, Integer>>(); 
		
		Map<String, Map<String, Integer>> hilberQgramTokenInvertedMap = 
			new HashMap<String, Map<String, Integer>>(); 
		
		//	build the inverted index
		this._rootNode.buildInvertedIndexNew(
		        objectDatabase,
				gramLocationInvertedMap,
				hilberQgramTokenInvertedMap,
				secondLevelInvertedIndex,
				hilbertQgramTokenInvertedIndex,				
				infrequentPositionalQgramSet,
				infrequentQgramTokenSet,
				sparseThreshold);
		
    // store the inveretedMap in to the disk;
    Iterator< Entry< String, Map<String, Integer> > > itr = gramLocationInvertedMap.entrySet().iterator(); 
    while ( itr.hasNext() )
    {
        Entry< String, Map<String, Integer> > entry = itr.next();
        String positionalQgram = entry.getKey();
        Map<String, Integer> locationMap = entry.getValue(); 
        
        // save in the firstLevelInvertedIndex
        firstLevelInvertedIndex.write(positionalQgram, locationMap);               
    }
    gramLocationInvertedMap.clear();
    
    
    itr = hilberQgramTokenInvertedMap.entrySet().iterator(); 
    while ( itr.hasNext() )
    {
        Entry< String, Map<String, Integer> > entry = itr.next();
        String positionalQgram = entry.getKey();
        Map<String, Integer> locationMap = entry.getValue(); 
        
        // save in the firstLevelInvertedIndex
        qgramTokenCountPairInvertedIndex.write(positionalQgram, locationMap);                   
    }
    hilberQgramTokenInvertedMap.clear();
	}
	
	
  public void getIntersectingNodeStatistic( 
      Region region,
      IntersectingNodeStatsMap intersectingNodeStatsMap, 
      int underLoadThreshold )
  {
    _rootNode.getIntersectingNodeStatistic( region, intersectingNodeStatsMap, underLoadThreshold );
  }
  
  
  public void getIntersectingNodeStatistic( 
      BooleanObject readBefore,
      Region region,
      IntersectingNodeStatsMap intersectingNodeStatsMap, 
      int underLoadThreshold )
  {
    if ( ! readBefore.isFalse() )
    {
      readBefore.setTrue();
      _rootNode.getIntersectingNodeStatistic( region, intersectingNodeStatsMap, underLoadThreshold );
    }    
  }
}