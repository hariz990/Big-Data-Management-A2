package spatialindex.quadtree;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import spatialindex.spatialindex.Point;
import spatialindex.spatialindex.Region;
import storage.invertedindex.FirstLevelInvertedIndex;
import storage.invertedindex.HilbertQgramTokenInvertedIndex;
import storage.invertedindex.QgramTokenCountPairInvertedIndex;
import storage.invertedindex.SecondLevelInvertedIndex;
import storage.objectindex.SpatialObjectDatabase;
import unit.HilbertCountMap;
import unit.IDSet;
import unit.IntersectingNodeStatsMap;
import unit.NodeStatistic;
import unit.PositionalQgram;
import unit.SpatialObject;

public class QuadTreeNode implements Serializable {
	

	private static final long serialVersionUID = 1L;

	protected static Logger log = Logger.getLogger(QuadTreeNode.class);

	public String _hilbertCode;
	
	public double[] _a;
	public double[] _b;
	public double[] _c;
	public double[] _d;
	
	public Region _region;
	public int _maxElements;
	public int _depth;
	public boolean _isLeaf;
	
	public ArrayList<QuadTreeNode> _childNodes;
	public ArrayList<QuadTreeObject> _objects;
	
	
	public QuadTreeNode(Region region, int depth) {
		this(region, depth, QuadTree.MAX_ELEMENTS);
	}

	public QuadTreeNode(Region region, int depth, int maxElements) {
		this._region = region;
		this._depth = depth;
		this._maxElements = maxElements;
		this._childNodes = new ArrayList<QuadTreeNode>();
		this._isLeaf = true;
		this._objects = new ArrayList<QuadTreeObject>();
		
//		log.debug("Creating new Node at depth " + depth);
	}

	public boolean isLeaf()
	{
		return _isLeaf;
	}
	
	public void setInternal()
	{
		_isLeaf = false;
	}
	
	/**
	 * Returns the startCoordinates for this Node
	 * 
	 * @return
	 */
	public Region getRegion() {
		return this._region;
	}

	/**
	 * Returns the subnodes of this node
	 * 
	 * @return
	 */
	public ArrayList<QuadTreeNode> getChildNodes() {
		return this._childNodes;
	}
	
	
	/**
	 * Returns the max elements
	 * 
	 * @return
	 */
	public int getMaxElements() {
		return this._maxElements;
	}

	/**
	 * Returns the depth of this node
	 * 
	 * @return
	 */
	public int getDepth() {
		return this._depth;
	}
	
	

	public QuadTreeNode getChildNode (Point p)
	{
		for (QuadTreeNode node : this._childNodes) 
		{			
			// if contains, return node
			if ( node._region.contains(p) ) {
				return node;
			}
		}
		return null;
	}
	
	
	public void insert (QuadTreeObject object)
	{
//		log.debug("Inserting element into Node at depth " + _depth);

		// if it is the internal node
		if ( ! isLeaf() ) 
		{
			QuadTreeNode node = getChildNode( object._point );
			node.insert(object);
		}

		// if it is the leaf node 
		else 
		{
			// Add the object to this node
			this._objects.add(object);
			
			// if the node needs to subdivide 
			if ( this._objects.size() > _maxElements) 
			{				
				// check whether the objects are the same, if they are the same, do not split						
				if ( ! isSameLocation() )
				{
					// split the node				
					this.subdivide();
					
					// Recall insert for each element. This will move all elements of
					// this node into the new nodes at the appropriate cell
					for (QuadTreeObject current : _objects) 
					{
						this.insert(current);
					}
					
					// Remove all elements from this node
					this._objects.clear();
				}
			}
		}
	}
	
	
	/**
	 * TODO: add the hilbert coding
	 */
	public void subdivide(){
//		log.debug("Subdividing node at depth " + _depth);
		
		// set the node to be internal node
		setInternal();
		
		QuadTreeNode cellNode = null;
		int depth = this._depth + 1;
		
		
		// need to check here for the correctness!!!!!!!!!!!!!!!!!!!!
		// new A
		double[] aa = this._a;
		double[] ab = getMiddle(this._a, this._d);
		double[] ac = getMiddle(this._a, this._c);
		double[] ad = getMiddle(this._a, this._b);
		
		// new B
		double[] ba = getMiddle(this._b, this._a);
		double[] bb = this._b;
		double[] bc = getMiddle(this._b, this._c);
		double[] bd = getMiddle(this._b, this._d);
		
		// new C
		double[] ca = getMiddle(this._c, this._a);
		double[] cb = getMiddle(this._c, this._b);
		double[] cc = this._c;
		double[] cd = getMiddle(this._c, this._d);
		
		// new D
		double[] da = getMiddle(this._d, this._c);
		double[] db = getMiddle(this._d, this._b);
		double[] dc = getMiddle(this._d, this._a);
		double[] dd = this._d;
		
		// new A
		cellNode = new QuadTreeNode ( getRegion(aa, ab, ac, ad), depth, this._maxElements );
		cellNode._hilbertCode = this._hilbertCode + Integer.toString(0);
		cellNode.setHibertCoor(aa, ab, ac, ad);		
		this._childNodes.add(0, cellNode);

		
		// new B
		cellNode = new QuadTreeNode ( getRegion(ba, bb, bc, bd), depth, this._maxElements );
		cellNode._hilbertCode = this._hilbertCode + Integer.toString(1);
		cellNode.setHibertCoor(ba, bb, bc, bd);
		this._childNodes.add(1, cellNode);
		
		// new C
		cellNode = new QuadTreeNode ( getRegion(ca, cb, cc, cd), depth, this._maxElements ); 
		cellNode._hilbertCode = this._hilbertCode + Integer.toString(2);
		cellNode.setHibertCoor(ca, cb, cc, cd);
		this._childNodes.add(2, cellNode);

		// new D
		cellNode = new QuadTreeNode ( getRegion(da, db, dc, dd), depth, this._maxElements );
		cellNode._hilbertCode = this._hilbertCode + Integer.toString(3);
		cellNode.setHibertCoor(da, db, dc, dd);
		this._childNodes.add(3, cellNode);

		
	}
	
	
	public void clear()
	{
		for (QuadTreeNode node : _childNodes)
		{
			node.clear();
		}
		_childNodes.clear();
	}

	public ArrayList<QuadTreeNode> getSubNodes()
	{
		return _childNodes;
	}

	public void getIntersectingNodeCoding(Region region, ArrayList<String> codes) {
		
		if ( !this._isLeaf )
		{
			for (QuadTreeNode node : this._childNodes) 
			{	
				// if intersects, recursively get the intersecting nodes
				if (node._region.intersects(region)) 
				{
					node.getIntersectingNodeCoding(region, codes);
				}
			}
		} else
		{
			codes.add(this._hilbertCode);
		}
	}
	
	public void getIntersectingLeafNode(Region region, ArrayList<QuadTreeNode> nodes)
	{		
		if ( !this._isLeaf )
		{
			for ( QuadTreeNode node : this._childNodes ) 
			{	
				// if intersects, recursively get the intersecting nodes
				if ( node._region.intersects( region ) ) 
				{
					node.getIntersectingLeafNode(region, nodes);
				}
			}
		} else {
			nodes.add(this);
		}		
	}
	
  /**
   * 
   * @param queryRegion
   * @param map
   * 
   *        this first value is the intersecting ratio, the second value is the number of objects in
   *        the leaf node
   */
  public void getIntersectingNodeStatistic( 
      Region queryRegion,
      IntersectingNodeStatsMap intersectingNodeStatsMap, 
      int underLoadThreshold )
  {
    if ( !this._isLeaf )
    {
      for ( QuadTreeNode node : this._childNodes )
      {
        // if intersects, recursively get the intersecting nodes
        if ( node._region.intersects( queryRegion ) )
        {
          node.getIntersectingNodeStatistic( queryRegion, intersectingNodeStatsMap, underLoadThreshold );
        }
      }
    }
    else
    {
      // save more thing is the node is not empty
      if ( this._objects.size() >= 1 )
      {
        NodeStatistic stats = new NodeStatistic();
        // save the number of objects in the node
        stats._numberOfObjects = this._objects.size();

        // the query region contains the node region
        if ( queryRegion.contains( this._region ) )
        {
          // set the intersecting ratio is 1
          stats._intersectingRatio = 1.0;

          // save the object id
          stats._intersectingObjectSet = new HashSet< Integer >();
          for ( QuadTreeObject object : this._objects )
          {
            stats._intersectingObjectSet.add( object._objectid );
          }
          // put it into the map
          intersectingNodeStatsMap.put( _hilbertCode, stats );
          
          
          
          // new add
          if ( stats._intersectingObjectSet.size() > underLoadThreshold )
          {
            intersectingNodeStatsMap.denseNodeNumber ++;
          }
        }
        // the query region intersects the node region
        else
        {
          // save the object id for the intersecting node
          stats._intersectingObjectSet = new HashSet< Integer >();
          stats._nonIntersectingObjectSet = new HashSet< Integer >();

          for ( QuadTreeObject object : this._objects )
          {
            // store the intersecting objects
            if ( queryRegion.contains( object._point ) )
            {
              stats._intersectingObjectSet.add( object._objectid );
            }
            // store the non-intersecting objects
            else
            {
              stats._nonIntersectingObjectSet.add( object._objectid );
            }
          }
          

          // set the intersecting ratio
          if ( ! stats._intersectingObjectSet.isEmpty() )
          {
            stats._intersectingRatio = stats._intersectingObjectSet.size() / (stats._numberOfObjects * 1.0);              
            intersectingNodeStatsMap.put( _hilbertCode, stats );
              
            if ( stats._intersectingObjectSet.size() > underLoadThreshold )
            {
              intersectingNodeStatsMap.denseNodeNumber ++;
            }
          }          
//          intersectingNodeStatsMap.put( _hilbertCode, stats );
        }     
      }
    }
  }
	
	
	public double[] getMiddle( double[] coor1, double[] coor2 )
	{
		return new double[] { ( coor1[0] + coor2[0] ) / 2, ( coor1[1] + coor2[1] ) / 2 };
	}
	
	
	public Region getRegion( double[] a, double[] b, double[] c, double[] d )
	{
		double xLow = Math.min( d[0], Math.min( c[0], Math.min( a[0], b[0] ) ) );
		double xHigh = Math.max( d[0], Math.max( c[0], Math.max( a[0], b[0] ) ) );
		double yLow = Math.min( d[1], Math.min( c[1], Math.min( a[1], b[1] ) ) );
		double yHigh = Math.max( d[1], Math.max( c[1], Math.max( a[1], b[1] ) ) );
		
		double[] low = new double[]{xLow, yLow};
		double[] high = new double[]{xHigh, yHigh};
		return new Region(low, high);
	}
	
	
	
	public void setHibertCoor(double[] a, double[] b, double[] c, double[] d)
	{
		this._a = a;
		this._b = b;
		this._c = c;
		this._d = d;
	}
	
	public boolean isSameLocation()
	{
		if ( this._objects.size() <= 1 )
			return true;
		
		Point firstPoint = this._objects.get(0).getPoint();
	
		for ( QuadTreeObject obj : this._objects )
		{
			if ( ! firstPoint.isCoLocation( obj.getPoint() ) )
			{
				return false;
			}
		}
		
		return true;
	}

	public void browse() {
		
		System.out.println(this._hilbertCode);
		if ( ! this.isLeaf() ) 
		{
			for ( QuadTreeNode node: this._childNodes )
			{
				node.browse();
			}
		}
	}
	
	
	public void buildInvertedIndex(
			HashMap<String, HilbertCountMap> gramLocationInvertedMap,
			SecondLevelInvertedIndex secondLevelInvertedIndex,
			SpatialObjectDatabase objectDatabase,
			HashSet<String> infrequentQGramSet, 
			final int underLoadThreshold,
			final int qgramLength) 
	{
		
		// if the visiting node is a internal node, visit its child nodes
		if ( ! this.isLeaf() ) 
		{
			for ( QuadTreeNode node: this._childNodes )
			{
				node.buildInvertedIndex(
						gramLocationInvertedMap,
						secondLevelInvertedIndex,
						objectDatabase,
						infrequentQGramSet, 
						underLoadThreshold,
						qgramLength);
			}
		}
				
		// if the visiting node is a leaf node, get its objects and build the inverted index.
		else 
		{
			if ( this._objects == null || this._objects.isEmpty() )
				return; 
		
			// if the node is a dense node
			if ( this._objects.size() > underLoadThreshold ) 
			{						
				HashMap<String, IDSet> invertedMap = new HashMap<String, IDSet>();
				
				for ( QuadTreeObject object : _objects )
				{
					int objectid = object._objectid;
					ArrayList<PositionalQgram> positionalQgramSet = 
						objectDatabase.getPositionalQgramSet( objectid );
						
					if ( positionalQgramSet == null )
					{
						log.fatal("Do not have obejct \" " + objectid + " \" in the object database ");
						return;
					}
						
					for ( PositionalQgram gram : positionalQgramSet )
					{									
						//	get the string representation of the positional q-gram
						String gramString = gram.toString();
						String gramToken = gram._qgram;
						int gramPosition = gram._pos;
						
						// 	frequent q-gram,
						//	need to save in the first-level and second-level inverted index
						if ( ! infrequentQGramSet.contains( gramString ) )
						{																			
							// 	save the q-gram info in the inverted database
							IDSet idSet = invertedMap.get( gramString );
							if ( idSet == null) 
							{
								// store the object id to the positional q-gram
								idSet = new IDSet( objectid );
								invertedMap.put( gramString, idSet );
									
								// store the position to the q-gram without position
								IDSet posSet = invertedMap.get(gramToken);
								if ( posSet == null )
								{
									posSet = new IDSet( gramPosition );
									invertedMap.put( gramToken, posSet );
								}
								else 
								{
									posSet.add( gramPosition );
								}
								
							}
							else 
							{
								// store the object id to the positional q-gram
								idSet.add( objectid );
									
								// store the position to the q-gram without position
								IDSet posSet = invertedMap.get( gramToken );
								posSet.add( gramPosition );	
							}																					
						}
						
						//	infrequent q-gram, need to store the first-level inverted index and
						//	the position list in the second-level inverted index  
						else
						{																				
							// store the (location, count) pair in the first level inverted index
							HilbertCountMap hilbertCountMap = gramLocationInvertedMap.get( gramString );						
							if ( hilbertCountMap == null)
							{	
								// store the hilbertCode
								hilbertCountMap = new HilbertCountMap();
								hilbertCountMap._map.put( this._hilbertCode, 1 );
								gramLocationInvertedMap.put( gramString, hilbertCountMap );								
							} 
							else 
							{
								// get the count and plus 1
								Integer count = hilbertCountMap._map.get( this._hilbertCode );
								if ( count == null )
								{
									hilbertCountMap._map.put( this._hilbertCode, 1 );
								}
								else
								{
									hilbertCountMap._map.put( this._hilbertCode, count + 1);
								}																
							}		
							
							
							//	save the position in the second level inverted index					
							IDSet positionSet = invertedMap.get( gramToken );
							if ( positionSet == null) 
							{
								// store the position to the q-gram token
								positionSet = new IDSet( gramPosition );
								invertedMap.put( gramToken, positionSet );
							}
							else
							{													
								// store the position to the q-gram token						
								positionSet.add( gramPosition );	
							}											
						}			
					}
				}	
				
				Iterator< Entry< String, IDSet > > itr = invertedMap.entrySet().iterator(); 				
				while ( itr.hasNext() )
				{
					Entry< String, IDSet > entry = itr.next();
					String gramString = entry.getKey();
					IDSet idSet = entry.getValue();					
																					
					//	q-gram token
					//	save the position list in the secondLevel inverted index
					if ( gramString.length() == qgramLength )
					{								
						String secondLevelKey = this._hilbertCode + "," + gramString;
						secondLevelInvertedIndex.put( secondLevelKey, idSet.getIntegerArray() );
					}
					
					//	positional q-gram
					else
					{				
						// 	save the count information in the firstLevelInvetedIndex
						HilbertCountMap hilbertCountMap = gramLocationInvertedMap.get( gramString );
						// 	if the hilbertCountMap is null, initialize a new one
						if ( hilbertCountMap == null) 
						{	
							hilbertCountMap = new HilbertCountMap();
							hilbertCountMap._map.put( this._hilbertCode, idSet.size() );
							gramLocationInvertedMap.put( gramString, hilbertCountMap );
						} 						
						else
						{
							hilbertCountMap._map.put( this._hilbertCode, idSet.size() );
						}
						
						String secondLevelKey = this._hilbertCode + "," + gramString;
						secondLevelInvertedIndex.put(secondLevelKey, idSet.getIntegerArray());
					}										
					itr.remove();
				}
			}
			
			//	sparse node
			//	only store the first-level inverted index for the query estimation
			else
			{												
				for ( QuadTreeObject object : _objects )
				{
					int objectid = object._objectid;
					ArrayList<PositionalQgram> positionalQgramSet = 
						objectDatabase.getPositionalQgramSet( objectid );
						
					if ( positionalQgramSet == null )
					{
						log.fatal("Do not have obejct \" " + objectid + " \" in the object database ");
						return;
					}
						
					for ( PositionalQgram gram : positionalQgramSet )
					{									
						String gramString = gram.toString();															
						// store the (location, count) pair in the first level inverted index
						HilbertCountMap hilbertCountMap = gramLocationInvertedMap.get( gramString );						
						if ( hilbertCountMap == null) 
						{	
							// store the hilbertCode
							hilbertCountMap = new HilbertCountMap();
							hilbertCountMap._map.put( this._hilbertCode, 1 );
							gramLocationInvertedMap.put( gramString, hilbertCountMap );								
						} 
						else 
						{
							// get the count and plus 1
							Integer count = hilbertCountMap._map.get( this._hilbertCode );
							if ( count == null )
							{
								hilbertCountMap._map.put( this._hilbertCode, 1 );
							}
							else
							{
								hilbertCountMap._map.put( this._hilbertCode, count + 1);
							}																
						}					
					}
				}						
			}
		}	
	}
	
	
	public void buildInvertedIndex(
			HashMap<String, HilbertCountMap> gramLocationInvertedMap,
			HashMap<String, HilbertCountMap> oneGramInvertedMap,
			SecondLevelInvertedIndex secondLevelInvertedIndex,
			SpatialObjectDatabase objectDatabase,
			HashSet<String> infrequentQGramSet, 
			final int underLoadThreshold,
			final int qgramLength) 
	{
		
		// if the visiting node is a internal node, visit its child nodes
		if ( ! this.isLeaf() ) 
		{
			for ( QuadTreeNode node: this._childNodes )
			{
				node.buildInvertedIndex(
						gramLocationInvertedMap,
						oneGramInvertedMap,
						secondLevelInvertedIndex,
						objectDatabase,
						infrequentQGramSet, 
						underLoadThreshold,
						qgramLength);
			}
		}
				
		// if the visiting node is a leaf node, get its objects and build the inverted index.
		else 
		{
			if ( this._objects == null || this._objects.isEmpty() )
				return; 
		
			// if the node is a dense node
			if ( this._objects.size() > underLoadThreshold ) 
			{						
				HashMap<String, IDSet> invertedMap = new HashMap<String, IDSet>();
				
				for ( QuadTreeObject object : _objects )
				{
					int objectid = object._objectid;
					ArrayList<PositionalQgram> positionalQgramSet = 
						objectDatabase.getPositionalQgramSet( objectid );
						
					if ( positionalQgramSet == null )
					{
						log.fatal("Do not have obejct \" " + objectid + " \" in the object database ");
						return;
					}
						
					for ( PositionalQgram gram : positionalQgramSet )
					{									
						//	get the string representation of the positional q-gram
						String gramString = gram.toString();
						String gramToken = gram._qgram;
						int gramPosition = gram._pos;
						
						// 	frequent q-gram,
						//	need to save in the first-level and second-level inverted index
						if ( ! infrequentQGramSet.contains( gramString ) )
						{																			
							// 	save the q-gram info in the inverted database
							IDSet idSet = invertedMap.get( gramString );
							if ( idSet == null) 
							{
								// store the object id to the positional q-gram
								idSet = new IDSet( objectid );
								invertedMap.put( gramString, idSet );
									
								// store the position to the q-gram without position
								IDSet posSet = invertedMap.get(gramToken);
								if ( posSet == null )
								{
									posSet = new IDSet( gramPosition );
									invertedMap.put( gramToken, posSet );
								}
								else 
								{
									posSet.add( gramPosition );
								}
								
							}
							else 
							{
								// store the object id to the positional q-gram
								idSet.add( objectid );
									
								// store the position to the q-gram without position
								IDSet posSet = invertedMap.get( gramToken );
								posSet.add( gramPosition );	
							}																					
						}
						
						//	infrequent q-gram, need to store the first-level inverted index and
						//	the position list in the second-level inverted index  
						else
						{																				
							// store the (location, count) pair in the first level inverted index
							HilbertCountMap hilbertCountMap = gramLocationInvertedMap.get( gramString );						
							if ( hilbertCountMap == null)
							{	
								// store the hilbertCode
								hilbertCountMap = new HilbertCountMap();
								hilbertCountMap._map.put( this._hilbertCode, 1 );
								gramLocationInvertedMap.put( gramString, hilbertCountMap );								
							} 
							else 
							{
								// get the count and plus 1
								Integer count = hilbertCountMap._map.get( this._hilbertCode );
								if ( count == null )
								{
									hilbertCountMap._map.put( this._hilbertCode, 1 );
								}
								else
								{
									hilbertCountMap._map.put( this._hilbertCode, count + 1);
								}																
							}		
							
							
							//	save the position in the second level inverted index					
							IDSet positionSet = invertedMap.get( gramToken );
							if ( positionSet == null) 
							{
								// store the position to the q-gram token
								positionSet = new IDSet( gramPosition );
								invertedMap.put( gramToken, positionSet );
							}
							else
							{													
								// store the position to the q-gram token						
								positionSet.add( gramPosition );	
							}											
						}
			
						
						/*
						 *	newly add for the 1-gram
						 */
						if ( gram._pos >= 0 )
						{
							String oneGram = gram._qgram.substring(0, 1) + gram._pos;
							HilbertCountMap countMap = oneGramInvertedMap.get( oneGram );						
							if ( countMap == null) 
							{	
								// store the hilbertCode
								countMap = new HilbertCountMap();
								countMap._map.put( this._hilbertCode, 1 );
								oneGramInvertedMap.put( oneGram, countMap );								
							} 
							else 
							{
								// get the count and plus 1
								Integer count = countMap._map.get( this._hilbertCode );
								if ( count == null )
								{
									countMap._map.put( this._hilbertCode, 1 );
								}
								else
								{
									countMap._map.put( this._hilbertCode, count + 1);
								}																
							}			
						}
						
						
					}
				}	
				
				Iterator< Entry< String, IDSet > > itr = invertedMap.entrySet().iterator(); 				
				while ( itr.hasNext() )
				{
					Entry< String, IDSet > entry = itr.next();
					String gramString = entry.getKey();
					IDSet idSet = entry.getValue();					
																					
					//	q-gram token
					//	save the position list in the secondLevel inverted index
					if ( gramString.length() == qgramLength )
					{								
						String secondLevelKey = this._hilbertCode + "," + gramString;
						secondLevelInvertedIndex.put( secondLevelKey, idSet.getIntegerArray() );
					}
					
					//	positional q-gram
					else
					{				
						// 	save the count information in the firstLevelInvetedIndex
						HilbertCountMap hilbertCountMap = gramLocationInvertedMap.get( gramString );
						// 	if the hilbertCountMap is null, initialize a new one
						if ( hilbertCountMap == null) 
						{	
							hilbertCountMap = new HilbertCountMap();
							hilbertCountMap._map.put( this._hilbertCode, idSet.size() );
							gramLocationInvertedMap.put( gramString, hilbertCountMap );
						} 						
						else
						{
							hilbertCountMap._map.put( this._hilbertCode, idSet.size() );
						}
						
						String secondLevelKey = this._hilbertCode + "," + gramString;
						secondLevelInvertedIndex.put(secondLevelKey, idSet.getIntegerArray());
					}										
					itr.remove();
				}
			}
			
			//	sparse node
			//	only store the first-level inverted index for the query estimation
			else
			{												
				for ( QuadTreeObject object : _objects )
				{
					int objectid = object._objectid;
					ArrayList<PositionalQgram> positionalQgramSet = 
						objectDatabase.getPositionalQgramSet( objectid );
						
					if ( positionalQgramSet == null )
					{
						log.fatal("Do not have obejct \" " + objectid + " \" in the object database ");
						return;
					}
						
					for ( PositionalQgram gram : positionalQgramSet )
					{									
						String gramString = gram.toString();															
						// store the (location, count) pair in the first level inverted index
						HilbertCountMap hilbertCountMap = gramLocationInvertedMap.get( gramString );						
						if ( hilbertCountMap == null) 
						{	
							// store the hilbertCode
							hilbertCountMap = new HilbertCountMap();
							hilbertCountMap._map.put( this._hilbertCode, 1 );
							gramLocationInvertedMap.put( gramString, hilbertCountMap );								
						} 
						else 
						{
							// get the count and plus 1
							Integer count = hilbertCountMap._map.get( this._hilbertCode );
							if ( count == null )
							{
								hilbertCountMap._map.put( this._hilbertCode, 1 );
							}
							else
							{
								hilbertCountMap._map.put( this._hilbertCode, count + 1);
							}																
						}	
			
						
						/*
						 *	newly add for the 1-gram
						 */
						if ( gram._pos >= 0 )
						{
							String oneGram = gram._qgram.substring(0, 1) + gram._pos;
							HilbertCountMap countMap = oneGramInvertedMap.get( oneGram );						
							if ( countMap == null) 
							{	
								// store the hilbertCode
								countMap = new HilbertCountMap();
								countMap._map.put( this._hilbertCode, 1 );
								oneGramInvertedMap.put( oneGram, countMap );								
							} 
							else 
							{
								// get the count and plus 1
								Integer count = countMap._map.get( this._hilbertCode );
								if ( count == null )
								{
									countMap._map.put( this._hilbertCode, 1 );
								}
								else
								{
									countMap._map.put( this._hilbertCode, count + 1);
								}																
							}			
						}
						
						
					}
				}						
			}
		}	
	}
	
	
	
	
  public void buildInvertedIndexNew( SpatialObjectDatabase objectDatabase,
      Map< String, Map<String, Integer> > firstLevelInvertedMap,
      Map< String, Map<String, Integer> > hilberQgramTokenInvertedMap,
      SecondLevelInvertedIndex secondLevelInvertedIndex,
      HilbertQgramTokenInvertedIndex hilbertQgramTokenInvertedIndex,
      HashSet< String > infrequentPositionQGramSet, 
      HashSet< String > infrequentQgramTokenSet,
      int sparseThreshold )
  {

    // if the visiting node is a internal node, visit its child nodes
    if ( !this.isLeaf() )
    {
      for ( QuadTreeNode node : this._childNodes )
      {
        node.buildInvertedIndexNew( objectDatabase, firstLevelInvertedMap,
            hilberQgramTokenInvertedMap, secondLevelInvertedIndex, hilbertQgramTokenInvertedIndex,
            infrequentPositionQGramSet, infrequentQgramTokenSet, sparseThreshold );
      }
    }

    // if the visiting node is a leaf node, get its objects and build the inverted index.
    else
    {
      if ( this._objects == null || this._objects.isEmpty() )
      {
        return;
      }
     
      // dense node
      if ( this._objects.size() > sparseThreshold )
      { 
        // for each node
        HashMap< String, IDSet > positionalQgramInvertedMap = new HashMap< String, IDSet >();
        HashMap< String, IDSet > qgramTokenInvertedMap = new HashMap< String, IDSet >();
        
        HashMap< String, Integer > positionalQgramCountMap = new HashMap< String, Integer >();
        HashMap< String, Integer > qgramTokenCountMap = new HashMap< String, Integer >();
        
        for ( QuadTreeObject object : _objects )
        {
          int objectid = object._objectid;
          SpatialObject spatialObject = objectDatabase.getSpatialObject( objectid );
  
          // get first M positional q-grams
          ArrayList< PositionalQgram > positionalQgramSet = objectDatabase.getFirstMPositionalQgram( spatialObject );
          // get q-gram token for larger q value
          HashSet< String > qgramTokenSet = objectDatabase.getQgramHashSetForLargerQ( spatialObject );
  
                   
          // store for first m positional q-grams
          if ( positionalQgramSet != null && ! positionalQgramSet.isEmpty() )
          {
            for ( PositionalQgram posQgram : positionalQgramSet )
            {
              // get the string representation of the positional q-gram
              String posQgramString = posQgram.toString();
  
              // frequent one
              if ( ! infrequentPositionQGramSet.contains( posQgramString ) )
              {
                // save the q-gram info in the inverted database
                IDSet idSet = positionalQgramInvertedMap.get( posQgramString );
                if ( idSet == null )
                {
                  // store the object id to the positional q-gram
                  idSet = new IDSet( objectid );
                  positionalQgramInvertedMap.put( posQgramString, idSet );
                }
                else
                {
                  // store the object id to the positional q-gram
                  idSet.add( objectid );
                }
              }
              // infrequent one
              else
              {
                // save the q-gram info in the inverted database
                Integer count = positionalQgramCountMap.get( posQgramString );
                if ( count == null )
                {             
                  positionalQgramCountMap.put( posQgramString, 1 );
                }
                else
                {
                  positionalQgramCountMap.put( posQgramString, count + 1 );
                }    
              }            
            }  
          }
                  
          // for q-gram         
          if ( qgramTokenSet != null && ! qgramTokenSet.isEmpty() )
          {
            for ( String token : qgramTokenSet )
            {                       
              // frequent case
              if ( ! infrequentQgramTokenSet.contains( token ) )
              {
                // save the q-gram info in the inverted database
                IDSet idSet = qgramTokenInvertedMap.get( token );
                if ( idSet == null )
                {
                  // store the object id to the positional q-gram
                  idSet = new IDSet( objectid );
                  qgramTokenInvertedMap.put( token, idSet );
                }
                else
                {
                  // store the object id to the positional q-gram
                  idSet.add( objectid );
                }   
              }
              // infrequent case
              else
              {
                Integer count = qgramTokenCountMap.get( token );
                if ( count == null )
                {                          
                  qgramTokenCountMap.put( token, 1 );
                }
                else
                {
                  // store the object id to the positional q-gram
                  qgramTokenCountMap.put( token, count + 1 );
                }       
              }                      
            }
          }
        }
        
          
        // save the frequent positional q-grams
        Iterator< Entry< String, IDSet >> itr = positionalQgramInvertedMap.entrySet().iterator();
        while ( itr.hasNext() )
        {
          Entry< String, IDSet > entry = itr.next();
          String posQgramString = entry.getKey();
          IDSet idSet = entry.getValue();
      
          String secondLevelKey = this._hilbertCode + "," + posQgramString;
          secondLevelInvertedIndex.put( secondLevelKey, idSet.getIntegerArray() );      

          // for first-level inverted index
          Map<String, Integer> hilbertCountMap = firstLevelInvertedMap.get( posQgramString );
          if ( hilbertCountMap == null )
          {
            // store the hilbertCode
            hilbertCountMap = new HashMap<String, Integer>();
            hilbertCountMap.put( this._hilbertCode, idSet.size() );
            firstLevelInvertedMap.put( posQgramString, hilbertCountMap );
          }
          else
          {
            // save the count
            hilbertCountMap.put( this._hilbertCode, idSet.size() );
          }
        }
        positionalQgramInvertedMap.clear();
        positionalQgramInvertedMap = null;


        // save the frequent q-gram tokens
        itr = qgramTokenInvertedMap.entrySet().iterator();
        while ( itr.hasNext() )
        {
          Entry< String, IDSet > entry = itr.next();
          String token = entry.getKey();
          IDSet idSet = entry.getValue();
         
          String secondLevelKey = this._hilbertCode + "," + token;
          hilbertQgramTokenInvertedIndex.write( secondLevelKey, idSet );
          
          // save it in the count map
          Map<String, Integer> hilbertCountMap = hilberQgramTokenInvertedMap.get( token );
          if ( hilbertCountMap == null )
          {
            // store the hilbertCode
            hilbertCountMap = new HashMap<String, Integer>();
            hilbertCountMap.put( this._hilbertCode, idSet.size() );
            hilberQgramTokenInvertedMap.put( token, hilbertCountMap );
          }
          else
          {
            hilbertCountMap.put( this._hilbertCode, idSet.size() );
          }

          // for small token
          String smallToken = token.substring( 0, objectDatabase._gen._len );
          Map<String, Integer> smallTokenHilbertCountMap = hilberQgramTokenInvertedMap.get( smallToken );
          if ( smallTokenHilbertCountMap == null )
          {
            smallTokenHilbertCountMap = new HashMap<String, Integer>();
            smallTokenHilbertCountMap.put( this._hilbertCode, idSet.size() );
            hilberQgramTokenInvertedMap.put( smallToken, smallTokenHilbertCountMap );
          }
          else
          {
            // get the count and plus 1
            Integer count = smallTokenHilbertCountMap.get( this._hilbertCode );
            if ( count == null )
            {
              smallTokenHilbertCountMap.put( this._hilbertCode, idSet.size() );
            }
            else
            {
              smallTokenHilbertCountMap.put( this._hilbertCode, count + idSet.size() );
            }
          }
        }
        qgramTokenInvertedMap.clear();  
        qgramTokenInvertedMap = null;
        
        
        
        // save for the (hilbert, count) pair
        Iterator< Entry< String, Integer >> countitr = positionalQgramCountMap.entrySet().iterator();
        while ( countitr.hasNext() )
        {
          Entry< String, Integer > entry = countitr.next();
          String gramString = entry.getKey();
          int size = entry.getValue();

          // for first-level inverted index
          Map<String, Integer> hilbertCountMap = firstLevelInvertedMap.get( gramString );
          if ( hilbertCountMap == null )
          {
            // store the hilbertCode
            hilbertCountMap = new HashMap<String, Integer>();
            hilbertCountMap.put( this._hilbertCode, size );
            firstLevelInvertedMap.put( gramString, hilbertCountMap );
          }
          else
          {
            // save the count
            hilbertCountMap.put( this._hilbertCode, size );
          }
        }
        positionalQgramCountMap.clear();
        positionalQgramCountMap = null;

        
        // save for the (hilbert, count) pair
        countitr = qgramTokenCountMap.entrySet().iterator();
        while ( countitr.hasNext() )
        {
          Entry< String, Integer > entry = countitr.next();
          String token = entry.getKey();
          int size = entry.getValue();

          // save the count for the node
          Map<String, Integer> hilbertCountMap = hilberQgramTokenInvertedMap.get( token );
          if ( hilbertCountMap == null )
          {
            // store the hilbertCode
            hilbertCountMap = new HashMap<String, Integer>();
            hilbertCountMap.put( this._hilbertCode, size );
            hilberQgramTokenInvertedMap.put( token, hilbertCountMap );
          }
          else
          {
            hilbertCountMap.put( this._hilbertCode, size );
          }

          // for small token
          String smallToken = token.substring( 0, objectDatabase._gen._len );
          Map<String, Integer> smallTokenHilbertCountMap = hilberQgramTokenInvertedMap.get( smallToken );
          if ( smallTokenHilbertCountMap == null )
          {
            smallTokenHilbertCountMap = new HashMap<String, Integer>();
            smallTokenHilbertCountMap.put( this._hilbertCode, size );
            hilberQgramTokenInvertedMap.put( smallToken, smallTokenHilbertCountMap );
          }
          else
          {
            // get the count and plus the count
            Integer count = smallTokenHilbertCountMap.get( this._hilbertCode );
            if ( count == null )
            {
              smallTokenHilbertCountMap.put( this._hilbertCode, size );
            }
            else
            {
              smallTokenHilbertCountMap.put( this._hilbertCode, count + size );
            }
          }
        }
        qgramTokenCountMap.clear();
        qgramTokenCountMap = null;
        
      }

      // sparse node, only store the ( hilbert, count ) pair
      else
      {
        HashMap< String, Integer > positionalQgramInvertedMap = new HashMap< String, Integer >();
        HashMap< String, Integer > qgramTokenInvertedMap = new HashMap< String, Integer >();
        
        for ( QuadTreeObject object : _objects )
        {
          int objectid = object._objectid;
          SpatialObject spatialObject = objectDatabase.getSpatialObject( objectid );
  
          // get first M positional q-grams
          ArrayList< PositionalQgram > positionalQgramSet = objectDatabase.getFirstMPositionalQgram( spatialObject );
          // get q-gram token for larger q value
          HashSet< String > qgramTokenSet = objectDatabase.getQgramHashSetForLargerQ( spatialObject );
       
          
          // store for first m positional q-grams
          if ( positionalQgramSet != null && ! positionalQgramSet.isEmpty() )
          {
            for ( PositionalQgram gram : positionalQgramSet )
            {
              // get the string representation of the positional q-gram
              String gramString = gram.toString();
  
              // save the q-gram info in the inverted database
              Integer count = positionalQgramInvertedMap.get( gramString );
              if ( count == null )
              {             
                positionalQgramInvertedMap.put( gramString, 1 );
              }
              else
              {
                positionalQgramInvertedMap.put( gramString, count + 1 );
              }                     
            }              
          }
    
          // for q-gram         
          if ( qgramTokenSet != null && ! qgramTokenSet.isEmpty() )
          {
            for ( String token : qgramTokenSet )
            {         
              // save the q-gram info in the inverted database
              Integer count = qgramTokenInvertedMap.get( token );
              if ( count == null )
              {                          
                qgramTokenInvertedMap.put( token, 1 );
              }
              else
              {
                // store the object id to the positional q-gram
                qgramTokenInvertedMap.put( token, count + 1 );
              }            
            }
          }
        }
        
        Iterator< Entry< String, Integer >> itr = positionalQgramInvertedMap.entrySet().iterator();
        while ( itr.hasNext() )
        {
          Entry< String, Integer > entry = itr.next();
          String gramString = entry.getKey();
          int size = entry.getValue();

          // for first-level inverted index
          Map<String, Integer> hilbertCountMap = firstLevelInvertedMap.get( gramString );
          if ( hilbertCountMap == null )
          {
            // store the hilbertCode
            hilbertCountMap = new HashMap<String, Integer>();
            hilbertCountMap.put( this._hilbertCode, size );
            firstLevelInvertedMap.put( gramString, hilbertCountMap );
          }
          else
          {
            // save the count
            hilbertCountMap.put( this._hilbertCode, size );
          }
        }
        positionalQgramInvertedMap.clear();
        positionalQgramInvertedMap = null;

        // save the frequent q-gram tokens
        itr = qgramTokenInvertedMap.entrySet().iterator();
        while ( itr.hasNext() )
        {
          Entry< String, Integer > entry = itr.next();
          String token = entry.getKey();
          int size = entry.getValue();

          // save the count for the node
          Map<String, Integer> hilbertCountMap = hilberQgramTokenInvertedMap.get( token );
          if ( hilbertCountMap == null )
          {
            // store the hilbertCode
            hilbertCountMap = new HashMap<String, Integer>();
            hilbertCountMap.put( this._hilbertCode, size );
            hilberQgramTokenInvertedMap.put( token, hilbertCountMap );
          }
          else
          {
            hilbertCountMap.put( this._hilbertCode, size );
          }

          // for small token
          String smallToken = token.substring( 0, objectDatabase._gen._len );
          Map<String, Integer> smallTokenHilbertCountMap = hilberQgramTokenInvertedMap.get( smallToken );
          if ( smallTokenHilbertCountMap == null )
          {
            smallTokenHilbertCountMap = new HashMap<String, Integer>();
            smallTokenHilbertCountMap.put( this._hilbertCode, size );
            hilberQgramTokenInvertedMap.put( smallToken, smallTokenHilbertCountMap );
          }
          else
          {
            // get the count and plus the count
            Integer count = smallTokenHilbertCountMap.get( this._hilbertCode );
            if ( count == null )
            {
              smallTokenHilbertCountMap.put( this._hilbertCode, size );
            }
            else
            {
              smallTokenHilbertCountMap.put( this._hilbertCode, count + size );
            }
          }
        }
        qgramTokenInvertedMap.clear();    
        qgramTokenInvertedMap = null;
        
      }
    }
      
          

          // store the (location, count) pair in the first level inverted index
//          HilbertCountMap hilbertCountMap = firstLevelInvertedMap.get( gramString );
//          if ( hilbertCountMap == null )
//          {
//            // store the hilbertCode
//            hilbertCountMap = new HilbertCountMap();
//            hilbertCountMap._map.put( this._hilbertCode, 1 );
//            firstLevelInvertedMap.put( gramString, hilbertCountMap );
//          }
//          else
//          {
//            // get the count and plus 1
//            Integer count = hilbertCountMap._map.get( this._hilbertCode );
//            if ( count == null )
//            {
//              hilbertCountMap._map.put( this._hilbertCode, 1 );                 
//            }
//            else
//            {
//              hilbertCountMap._map.put( this._hilbertCode, count + 1 );
//            }
//          }
       

       

          // store the (location, count) pair in the first level inverted index
//          HilbertCountMap hilbertCountMap = hilberQgramTokenInvertedMap.get( token );
//          if ( hilbertCountMap == null )
//          {
//            // store the hilbertCode
//            hilbertCountMap = new HilbertCountMap();
//            hilbertCountMap._map.put( this._hilbertCode, 1 );
//            hilberQgramTokenInvertedMap.put( token, hilbertCountMap );
//          }
//          else
//          {
//            // get the count and plus 1
//            Integer count = hilbertCountMap._map.get( this._hilbertCode );
//            if ( count == null )
//            {
//              hilbertCountMap._map.put( this._hilbertCode, 1 );
//            }
//            else
//            {
//              hilbertCountMap._map.put( this._hilbertCode, count + 1 );
//            }
//          }

          
          // save the Hilbert count pair for smaller q value
//          String smallToken = token.substring( 0, objectDatabase._gen._len );
//          HilbertCountMap smallTokenHilbertCountMap = hilberQgramTokenInvertedMap.get( smallToken );
//          if ( smallTokenHilbertCountMap == null )
//          {
//            smallTokenHilbertCountMap = new HilbertCountMap();
//            smallTokenHilbertCountMap._map.put( this._hilbertCode, 1 );
//            hilberQgramTokenInvertedMap.put( smallToken, smallTokenHilbertCountMap );
//          }
//          else
//          {
//            // get the count and plus 1
//            Integer count = smallTokenHilbertCountMap._map.get( this._hilbertCode );
//            if ( count == null )
//            {
//              smallTokenHilbertCountMap._map.put( this._hilbertCode, 1 );
//            }
//            else
//            {
//              smallTokenHilbertCountMap._map.put( this._hilbertCode, count + 1 );
//            }
//          }
        
      

    
      
    
  }
  
  
  
  
  public void buildInvertedIndexMem( 
      SpatialObjectDatabase objectDatabase,
      FirstLevelInvertedIndex firstLevelInvertedMap,
      QgramTokenCountPairInvertedIndex hilberQgramTokenInvertedMap,
      SecondLevelInvertedIndex secondLevelInvertedIndex,
      HilbertQgramTokenInvertedIndex hilbertQgramTokenInvertedIndex,
      HashSet< String > infrequentPositionQGramSet, HashSet< String > infrequentQgramTokenSet,
      int sparseThreshold )
  {

    // if the visiting node is a internal node, visit its child nodes
    if ( !this.isLeaf() )
    {
      for ( QuadTreeNode node : this._childNodes )
      {
        node.buildInvertedIndexMem( objectDatabase, firstLevelInvertedMap,
            hilberQgramTokenInvertedMap, secondLevelInvertedIndex, hilbertQgramTokenInvertedIndex,
            infrequentPositionQGramSet, infrequentQgramTokenSet, sparseThreshold );
      }
    }

    // if the visiting node is a leaf node, get its objects and build the inverted index.
    else
    {
      if ( this._objects == null || this._objects.isEmpty() ) return;

      HashMap< String, IDSet > positionalQgramInvertedMap = new HashMap< String, IDSet >();
      HashMap< String, IDSet > qgramTokenInvertedMap = new HashMap< String, IDSet >();

      for ( QuadTreeObject object : _objects )
      {
        int objectid = object._objectid;
        SpatialObject spatialObject = objectDatabase.getSpatialObject( objectid );

        // get first M positional q-grams
        ArrayList< PositionalQgram > positionalQgramSet =
            objectDatabase.getFirstMPositionalQgram( spatialObject );

        if ( positionalQgramSet == null )
        {
          log.fatal( "Do not have obejct \" " + objectid + " \" in the object database " );
          return;
        }


        // store for first m positional q-grams
        for ( PositionalQgram gram : positionalQgramSet )
        {
          // get the string representation of the positional q-gram
          String gramString = gram.toString();

          // only store for the dense node
          if ( this._objects.size() > sparseThreshold )
          {
            // frequent q-gram,
            // need to save in the second-level inverted index
            if ( !infrequentPositionQGramSet.contains( gramString ) )
            {
              // save the q-gram info in the inverted database
              IDSet idSet = positionalQgramInvertedMap.get( gramString );
              if ( idSet == null )
              {
                // store the object id to the positional q-gram
                idSet = new IDSet( objectid );
                positionalQgramInvertedMap.put( gramString, idSet );
              }
              else
              {
                // store the object id to the positional q-gram
                idSet.add( objectid );
              }
            }
          }

          // store the (location, count) pair in the first level inverted index
          HilbertCountMap hilbertCountMap = firstLevelInvertedMap.read( gramString );
          if ( hilbertCountMap == null )
          {
            // store the hilbertCode
            hilbertCountMap = new HilbertCountMap();
            hilbertCountMap._map.put( this._hilbertCode, 1 );
            firstLevelInvertedMap.write( gramString, hilbertCountMap );
          }
          else
          {
            // get the count and plus 1
            Integer count = hilbertCountMap._map.get( this._hilbertCode );
            if ( count == null )
            {
              hilbertCountMap._map.put( this._hilbertCode, 1 );

              // newly add
              firstLevelInvertedMap.write( gramString, hilbertCountMap );
            }
            else
            {
              hilbertCountMap._map.put( this._hilbertCode, count + 1 );

              // newly add
              firstLevelInvertedMap.write( gramString, hilbertCountMap );
            }
          }
        }

        // get q-gram token for larger q value
        HashSet< String > qgramSTokenSet = objectDatabase.getQgramHashSetForLargerQ( spatialObject );
        if ( qgramSTokenSet == null )
        {
          continue;
        }

        for ( String token : qgramSTokenSet )
        {
          // only store for the dense node
          if ( this._objects.size() > sparseThreshold )
          {
            // frequent q-gram,
            // need to save in the first-level and second-level inverted index
            if ( !infrequentQgramTokenSet.contains( token ) )
            {
              // save the q-gram info in the inverted database
              IDSet idSet = qgramTokenInvertedMap.get( token );
              if ( idSet == null )
              {
                // store the object id to the positional q-gram
                idSet = new IDSet( objectid );
                qgramTokenInvertedMap.put( token, idSet );
              }
              else
              {
                // store the object id to the positional q-gram
                idSet.add( objectid );
              }
            }
          }

          // store the (location, count) pair in the first level inverted index
          HilbertCountMap hilbertCountMap = hilberQgramTokenInvertedMap.read( token );
          if ( hilbertCountMap == null )
          {
            // store the hilbertCode
            hilbertCountMap = new HilbertCountMap();
            hilbertCountMap._map.put( this._hilbertCode, 1 );
            hilberQgramTokenInvertedMap.write( token, hilbertCountMap );
          }
          else
          {
            // get the count and plus 1
            Integer count = hilbertCountMap._map.get( this._hilbertCode );
            if ( count == null )
            {
              hilbertCountMap._map.put( this._hilbertCode, 1 );

              // newly add
              hilberQgramTokenInvertedMap.write( token, hilbertCountMap );
            }
            else
            {
              hilbertCountMap._map.put( this._hilbertCode, count + 1 );

              // newly add
              hilberQgramTokenInvertedMap.write( token, hilbertCountMap );
            }
          }


          // save the Hilbert count pair for smaller q value
          String smallToken = token.substring( 0, objectDatabase._gen._len );
          HilbertCountMap smallTokenHilbertCountMap = hilberQgramTokenInvertedMap.read( smallToken );
          if ( smallTokenHilbertCountMap == null )
          {
            smallTokenHilbertCountMap = new HilbertCountMap();
            smallTokenHilbertCountMap._map.put( this._hilbertCode, 1 );
            hilberQgramTokenInvertedMap.write( smallToken, smallTokenHilbertCountMap );
          }
          else
          {
            // get the count and plus 1
            Integer count = smallTokenHilbertCountMap._map.get( this._hilbertCode );
            if ( count == null )
            {
              smallTokenHilbertCountMap._map.put( this._hilbertCode, 1 );

              // newly add
              hilberQgramTokenInvertedMap.write( smallToken, smallTokenHilbertCountMap );
            }
            else
            {
              smallTokenHilbertCountMap._map.put( this._hilbertCode, count + 1 );

              // newly add
              hilberQgramTokenInvertedMap.write( smallToken, smallTokenHilbertCountMap );
            }
          }
        }
      }

      // save the positional q-grams
      Iterator< Entry< String, IDSet >> itr = positionalQgramInvertedMap.entrySet().iterator();
      while ( itr.hasNext() )
      {
        Entry< String, IDSet > entry = itr.next();
        String gramString = entry.getKey();
        IDSet idSet = entry.getValue();

        String secondLevelKey = this._hilbertCode + "," + gramString;
        secondLevelInvertedIndex.put( secondLevelKey, idSet.getIntegerArray() );
        itr.remove();
      }

      // save the q-gram tokens
      itr = qgramTokenInvertedMap.entrySet().iterator();
      while ( itr.hasNext() )
      {
        Entry< String, IDSet > entry = itr.next();
        String token = entry.getKey();
        IDSet idSet = entry.getValue();

        String secondLevelKey = this._hilbertCode + "," + token;

        if ( idSet == null )
        {
          System.err.println( secondLevelKey );
        }
        hilbertQgramTokenInvertedIndex.write( secondLevelKey, idSet );
        itr.remove();
      }
    }
  }

		
	public QuadTreeNode getLeafNode (String hilbertCode) 
	{
		QuadTreeNode node = this;
				
		//	start from 1, since the Hilbert code for root node is 0
//		for ( int i = 1; i < hilbertCode.length(); i++ )
		for ( int i = 0; i < hilbertCode.length(); i++ )
		{			
			node = node._childNodes.get( Integer.parseInt( hilbertCode.substring(i, i + 1) ) );	
		}
		
		return node;
	}
	
	
	public int getObjectNumberOfLeafNode ( String hilbertCode ) 
	{	
		QuadTreeNode node = this;
		
		//	start from 1, since the Hilbert code for root node is 0
//		for ( int i = 1; i < hilbertCode.length(); i++ )
		for ( int i = 0; i < hilbertCode.length(); i++ )
		{			
			int index = Integer.parseInt( hilbertCode.substring(i, i + 1) );								
			node = node._childNodes.get( index );
		}
		
		if ( ! node._isLeaf )
		{
			System.err.println("the hilbertCode does not correspond to a leaf node");
		}
		
		return node._objects.size();
	}

	public int getNumberOfLeaves()
	{	
		int num = 0;
		if ( this.isLeaf() )
		{
			return 1;
		}
		else
		{
			for ( QuadTreeNode node: this._childNodes )
			{
				num += node.getNumberOfLeaves();
			}
			return num;
		}
		
	}
	
	
	public int getMaxDepth()
	{
		int maxDepth = 0;
		if ( this.isLeaf() )
		{
			return this._depth;
		}
		else
		{
			for ( QuadTreeNode node: this._childNodes )
			{
				int childDepth = node.getMaxDepth();
				if ( childDepth > maxDepth )
				{
					maxDepth = childDepth;
				}
			}
			return maxDepth;
		}
		
	}

	public int getSparseNodeNumber(int sparseThreshold) 
	{
		int num = 0;
		if ( this.isLeaf() )
		{
			if ( this._objects.size() < sparseThreshold )
			{
				return 1;
			}
			else
			{
				return 0;
			}
		}
		else
		{
			for ( QuadTreeNode node: this._childNodes )
			{
				num += node.getSparseNodeNumber(sparseThreshold);
			}
			return num;
		}	
		
	}

}
