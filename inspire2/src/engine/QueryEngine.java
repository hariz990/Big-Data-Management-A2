package engine;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Map.Entry;

import filter.QgramFilter;
import sortmergejoin.IntersectionJoin;
import sortmergejoin.MergeSkip;
import sortmergejoin.NodeLevelJoin;
import sortmergejoin.SortMergeJoin;
import spatialindex.quadtree.QuadTree;
import spatialindex.spatialindex.Region;
import storage.invertedindex.FirstLevelInvertedIndex;
import storage.invertedindex.HilbertQgramTokenInvertedIndex;
import storage.invertedindex.InfrequentPositionalQgramInvertedIndex;
import storage.invertedindex.InfrequentQgramTokenInvertedIndex;
import storage.invertedindex.QgramTokenCountPairInvertedIndex;
import storage.invertedindex.SecondLevelInvertedIndex;
import storage.objectindex.SpatialObjectDatabase;
import stringverification.StringVerification;
import unit.BooleanObject;
import unit.HilbertCountMap;
import unit.IntersectingNodeStatsMap;
import unit.NodeStatistic;
import unit.PositionalQgram;
import unit.SecondLevelKey;
import unit.SpatialObject;
import unit.StopWatch;
import unit.query.QueryType;
import unit.query.SpatialQuery;


public class QueryEngine
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

  public int sparseThreshold;
  

  
  /**
   * newly add
   */
  public int selectivityThreshold;
  public int resultSizeThreshold;
  
  public int visitingNodeSizeThreshold;
  
  public double scarceThreshold;
  
  public double areaEnlargedRatio;  
  public int lamdaValue; // position upper bound

  
  
  public int getResultSizeThreshold()
  {
    return resultSizeThreshold;
  }

  
  public void setResultSizeThreshold( int resultSizeThreshold )
  {
    this.resultSizeThreshold = resultSizeThreshold;
  }

  
  public double getAreaEnlargedRatio()
  {
    return areaEnlargedRatio;
  }

  
  public void setAreaEnlargedRatio( double areaEnlargedRatio )
  {
    this.areaEnlargedRatio = areaEnlargedRatio;
  }
  
   

  public QueryEngine(QuadTree quadtree, 
      SpatialObjectDatabase objectDatabase,
      FirstLevelInvertedIndex firstLevelInvertedIndex,
      SecondLevelInvertedIndex secondLevelInvertedIndex,
      InfrequentPositionalQgramInvertedIndex infrequentInvertedIndex,
      InfrequentQgramTokenInvertedIndex infrequentTokenInvertedIndex,
      QgramTokenCountPairInvertedIndex qgramTokenCountPairInvertedIndex,
      HilbertQgramTokenInvertedIndex hilbertQgramTokenInvertedIndex,
      int sparseThreshold, 
      int selectivityThreshold,
      int resultSizeThreshold,
      int visitingNodeSizeThreshold, 
      double scarceThreshold,
      double areaEnlargedRatio,
      int lamdaValue)  
  {

    this.quadtree = quadtree;
    this.objectDatabase = objectDatabase;
    this.firstLevelInvertedIndex = firstLevelInvertedIndex;
    this.secondLevelInvertedIndex = secondLevelInvertedIndex;
    this.infrequentPosQgramInvertedIndex = infrequentInvertedIndex;
    this.infrequentTokenInvertedIndex = infrequentTokenInvertedIndex;
    this.qgramTokenCountPairInvertedIndex = qgramTokenCountPairInvertedIndex;
    this.hilbertQgramTokenInvertedIndex = hilbertQgramTokenInvertedIndex;
    
    
    this.sparseThreshold = sparseThreshold;
    this.selectivityThreshold = selectivityThreshold;
    this.resultSizeThreshold = resultSizeThreshold;
    this.visitingNodeSizeThreshold = visitingNodeSizeThreshold;
    
    this.scarceThreshold = scarceThreshold;
    this.areaEnlargedRatio = areaEnlargedRatio;
    this.lamdaValue = lamdaValue;
  }

  // add results
  public void addResults (       
      HashMap< Integer, SimpleEntry < QueryType, SpatialObject > > resultMap, 
      Set< Integer > resultList,
      HashMap< Integer, SpatialObject > memObjectMap,
      QueryType type )
  {        
    for ( int id : resultList )
    {    
      // if the result map does not contain the result
      if ( ! resultMap.containsKey( id ) ) 
      {
        // try to retrieve it from memObjectMap
        SpatialObject spatialObject = memObjectMap.get( id );
       
        // else retrieve it from the database
        if ( spatialObject == null )
        {
          spatialObject = objectDatabase.getSpatialObject( id );        
        }
        
        resultMap.put( id, new SimpleEntry < QueryType, SpatialObject >( type, spatialObject ) );
      }            
    }       
  }
  
  
  // add results
  public void addResults (       
      HashMap< Integer, SimpleEntry < QueryType, SpatialObject > > resultMap, 
      List< Integer > resultList,
      HashMap< Integer, SpatialObject > memObjectMap,
      QueryType type )
  {        
    for ( int id : resultList )
    {    
      // if the result map does not contain the result
      if ( ! resultMap.containsKey( id ) ) 
      {
        // try to retrieve it from memObjectMap
        SpatialObject spatialObject = memObjectMap.get( id );
       
        // else retrieve it from the database
        if ( spatialObject == null )
        {
          spatialObject = objectDatabase.getSpatialObject( id );        
        }
        
        resultMap.put( id, new SimpleEntry < QueryType, SpatialObject >( type, spatialObject ) );
      }            
    }       
  }
  
  
  

  public HashMap< Integer, SimpleEntry < QueryType, SpatialObject >> query( SpatialQuery query, StopWatch stopWatch )
  {
    // intersectingNodeStatsMap
    IntersectingNodeStatsMap intersectingNodeStatsMap = new IntersectingNodeStatsMap();
    
    // intersectingNodeStatsMapOfRelaxedRegion
    IntersectingNodeStatsMap intersectingNodeStatsMapOfRelaxedRegion = new IntersectingNodeStatsMap();

    // spatial objects that are visited before
    HashMap< Integer, SpatialObject > memObjectMap = new HashMap< Integer, SpatialObject >();

    // memory infrequent positional inverted map
    HashMap< PositionalQgram, TreeSet< Integer >> memInfreqPosQgramInvertedMap = new HashMap< PositionalQgram, TreeSet< Integer >>();

    // memory infrequent positional inverted map
    HashMap< String, TreeSet< Integer >> memInfreqQgramTokenInvertedMap = new HashMap< String, TreeSet< Integer >>();
    
    // memPosQgramCountPairInvertedMap
    HashMap< PositionalQgram, HilbertCountMap > memPosQgramCountPairInvertedMap = new HashMap< PositionalQgram, HilbertCountMap >();

    // memQgramTokenCountPairInvertedMap
    HashMap< String, HilbertCountMap > memQgramTokenCountPairInvertedMap = new HashMap< String, HilbertCountMap >();

    // memHilbPosQgramInvertedMap
    HashMap< SecondLevelKey, TreeSet< Integer >> memHilbPosQgramInvertedMap = new HashMap< SecondLevelKey, TreeSet< Integer >>();

    // memHilbPosQgramInvertedMap
    HashMap< String, TreeSet< Integer >> memHilbQgramTokenInvertedMap = new HashMap< String, TreeSet< Integer >>();

    // prefix query result in the given spatial range
    ArrayList< Integer > prefixResult = new ArrayList< Integer >();

    // intermediate result for the prefix query
    ArrayList< Integer > prefixResultFromInfrequentQgram = new ArrayList< Integer >();

    // substring candidates from the infrequent qgrams
    ArrayList< Integer > substringCandidates = new ArrayList< Integer >();

    // record what are the prefix objects in each hilbert node
    // is used in the prefix size estimation
    HashMap< String, TreeSet< Integer >> prefixResultMap = new HashMap< String, TreeSet< Integer >>();

    // record the join result of each node
    HashMap< String, TreeSet< Integer >> prefixResultFromNodeLevelJoin = new HashMap< String, TreeSet< Integer >>();

    // have read the infrequent token or not
    BooleanObject hasReadInfrequentToken = new BooleanObject( false );
    BooleanObject hasInfrequentPosQgram = new BooleanObject( false );
    BooleanObject hasInfrequentQgramToken = new BooleanObject( false );
    
    BooleanObject doPrefixNodeFilter = new BooleanObject( false );
    BooleanObject doSubstringNodeFilterInPrefixQuery = new BooleanObject( false );
    BooleanObject doSubstringNodeFilter = new BooleanObject( false );
           
    BooleanObject hasRetrievedIntersectingNodes = new BooleanObject( false );
    BooleanObject hasRetrievedIntersectingNodesOfRelaxedRegion = new BooleanObject( false );    
    BooleanObject hasReadSmallQgram = new BooleanObject( false );    
    
    TreeMap< String, ArrayList<Integer> > prefixNodeCandidateCountMap = new TreeMap< String, ArrayList<Integer> >();
    TreeMap< String, ArrayList<Integer> > substringNodeCandidateCountMap = new TreeMap< String, ArrayList<Integer> >();    
    HashMap< String, Double > substringSelectivityMap = new HashMap< String, Double >();     
    
    HashMap< Integer, SimpleEntry < QueryType, SpatialObject > > resultMap = new HashMap< Integer, SimpleEntry < QueryType, SpatialObject > >(); 
    
    stopWatch.prefixRangeCount++;        
    long spStart = System.currentTimeMillis();
    
    /**
     * prefix query in the given spatial range
     */    
    query._queryType = QueryType.PREFIX_RANGE;       
    this.prefixRangeQuery( query,  
        prefixResult, prefixResultFromInfrequentQgram,
        substringCandidates, prefixResultMap, prefixResultFromNodeLevelJoin,
        prefixNodeCandidateCountMap,
        substringNodeCandidateCountMap,
        hasReadInfrequentToken, 
        hasInfrequentPosQgram,
        hasInfrequentQgramToken,
        doPrefixNodeFilter,
        doSubstringNodeFilterInPrefixQuery,    
        hasRetrievedIntersectingNodes,
        hasRetrievedIntersectingNodesOfRelaxedRegion,
        intersectingNodeStatsMap, 
        intersectingNodeStatsMapOfRelaxedRegion,
        memObjectMap,
        memInfreqPosQgramInvertedMap, memInfreqQgramTokenInvertedMap,
        memPosQgramCountPairInvertedMap, memQgramTokenCountPairInvertedMap,
        memHilbPosQgramInvertedMap, memHilbQgramTokenInvertedMap );
    

    // add results,
    // if the result size is greater than the threshold, return the objects
    this.addResults( resultMap, prefixResult, memObjectMap, QueryType.PREFIX_RANGE );
    
    long spStop = System.currentTimeMillis();
    stopWatch.prefixRangeTime += ( spStop - spStart) ;
    
    if ( resultMap.size() >= query.getResultSizeThreshold() )
    {
      return resultMap;
    }           
      
    /**
     * substring query in the query range
     */               
    stopWatch.substringRangeCount ++;
    
    query._queryType = QueryType.SUBSTRING_RANGE;    
    HashSet< Integer > substringResult = new HashSet< Integer >();    
    substringResult.addAll( prefixResult );
    
    // if there are infrequent q-gram tokens
    if ( hasReadInfrequentToken.isTrue() && hasInfrequentQgramToken.isTrue() )      
    {            
      long substringRangeStartTime1 = System.currentTimeMillis();      
      
      browseSubstringInfrequentResult( query, substringCandidates, objectDatabase, substringResult, memObjectMap );
                      
      this.addResults( resultMap, substringResult, memObjectMap, QueryType.SUBSTRING_RANGE );
      
      stopWatch.substringRangeTime += ( System.currentTimeMillis() - substringRangeStartTime1 );
      
      if ( resultMap.size() >= query.getResultSizeThreshold() )
      {        
        return resultMap;
      }
    }
    else
    {
      // check whether have read the infrequent token
      if ( hasReadInfrequentToken.isFalse() )        
      {               
        
        long tempTime1 = System.currentTimeMillis();
        
        this.getInfrequentQgramTokenMapping( memInfreqQgramTokenInvertedMap, query._largeQgramTokenSet, hasReadInfrequentToken, hasInfrequentQgramToken );        
        
        stopWatch.substringRangeTime += ( System.currentTimeMillis() - tempTime1 );
        
        // has infrequent q-gram token
        if ( hasInfrequentQgramToken.isTrue() )
        {         
          
          long tempTime2 = System.currentTimeMillis();
          
          NavigableSet< Integer > joinedResult = infrequentQgramExactJoin( memInfreqQgramTokenInvertedMap.values() );      
          
          stopWatch.substringRangeTime += ( System.currentTimeMillis() - tempTime2 );
          
          if ( joinedResult != null )
          {                       
            long tempTime3 = System.currentTimeMillis();
            
            browseSubstringInfrequentResult( query, joinedResult, objectDatabase, substringResult, memObjectMap );                                             
           
            stopWatch.substringRangeTime += ( System.currentTimeMillis() - tempTime3 );
            
            this.addResults( resultMap, substringResult, memObjectMap, QueryType.SUBSTRING_RANGE );
            if ( resultMap.size() >= query.getResultSizeThreshold() )
            {        
              return resultMap;
            }
          }
        }
      }

      // check if the query text does not contain any infrequent q-gram token      
      if ( hasInfrequentQgramToken.isFalse() )
      {                
        
        long getIntersectingNodeStartTime = System.currentTimeMillis();
        
        // get the intersecting node statistics
        if ( hasRetrievedIntersectingNodes.isFalse() )
        {
          quadtree.getIntersectingNodeStatistic( query._queryRegion, intersectingNodeStatsMap, sparseThreshold );
          hasRetrievedIntersectingNodes.setTrue();
        }                
        
        long intersectingNodeTime = (System.currentTimeMillis() - getIntersectingNodeStartTime);
        
        // if the query text is too short, directly do substring query. no approximate query will be applied
        if ( query._queryText.length() <= 3 )
        {                                                                   
          long substringRangeStartTime4 = System.currentTimeMillis();
          
          // substring query
          substringRangeQuery( query, substringResult, substringNodeCandidateCountMap, intersectingNodeStatsMap, 
            doSubstringNodeFilterInPrefixQuery, doSubstringNodeFilter, false, memObjectMap, memHilbQgramTokenInvertedMap );          
          
          this.addResults( resultMap, substringResult, memObjectMap, QueryType.SUBSTRING_RANGE );
          
          stopWatch.substringRangeTime += ( System.currentTimeMillis() - substringRangeStartTime4 + intersectingNodeTime );
          
          if ( resultMap.size() >= query.getResultSizeThreshold() )
          {        
            return resultMap;
          }
        }

        // if the length of the query text is greater than 3
        // estimate the selectivity of the substring query first:SSE
        else
        {          
          long substringSizeEstimationStartTime = System.currentTimeMillis();
          
          // substring selectivity estimation                    
          boolean exceedSelectivityThreshold = isSubstringSelectivityExeedSelectivityThreshold( 
            query, doSubstringNodeFilterInPrefixQuery, doSubstringNodeFilter, hasReadSmallQgram,
            substringNodeCandidateCountMap, intersectingNodeStatsMap, prefixResultMap, 
            memQgramTokenCountPairInvertedMap, memHilbQgramTokenInvertedMap, substringSelectivityMap );         
          
          stopWatch.substringSizeEstimationCount ++;
          stopWatch.substringSizeEstimationTime += ( System.currentTimeMillis() - substringSizeEstimationStartTime + intersectingNodeTime);
          stopWatch.substringRangeTime += ( System.currentTimeMillis() - substringSizeEstimationStartTime + intersectingNodeTime );
          
          if ( exceedSelectivityThreshold )
          {       
            long tempTime4 = System.currentTimeMillis();
            substringRangeQuery( query, substringResult, substringNodeCandidateCountMap, intersectingNodeStatsMap, 
              doSubstringNodeFilterInPrefixQuery, doSubstringNodeFilter, false, memObjectMap, memHilbQgramTokenInvertedMap );                     
            this.addResults( resultMap, substringResult, memObjectMap, QueryType.SUBSTRING_RANGE );
            
            stopWatch.substringRangeTime += ( System.currentTimeMillis() - tempTime4 );
            
            if ( resultMap.size() >= query.getResultSizeThreshold() )
            {        
              return resultMap;
            }
          }          
        }
      }      
    }
    // get the intersecting node statistics    
    long getIntersectingNodeStartTime = System.currentTimeMillis();
    if ( hasRetrievedIntersectingNodes.isFalse() )
    {
      quadtree.getIntersectingNodeStatistic( query._queryRegion, intersectingNodeStatsMap, sparseThreshold );
      hasRetrievedIntersectingNodes.setTrue();
    }
    long intersectingNodeTime = System.currentTimeMillis() - getIntersectingNodeStartTime;

    /**
     * apply approximate prefix when the query length is greater than 3
     */
    if ( query._queryText.length() > 3 )
    {
      stopWatch.approximatePrefixRangeCount ++;
      long approximatePrefixRangeStartTime = System.currentTimeMillis();
      
      // approximate prefix query in the given spatial range      
      HashSet< Integer > approximatePrefixResult = new HashSet< Integer >();
      query._queryType = QueryType.APPROXIMATE_PREFIX_RANGE;      
                        
  
      // copy the prefix result      
      if ( prefixResult != null && ! prefixResult.isEmpty() )
      {        
        approximatePrefixResult.addAll( prefixResult );       
      }

      TreeSet< String > approximateSubstringNodeCandidates = new TreeSet< String >();
      BooleanObject doApproximateSubstringNodeFilter = new BooleanObject( false );      
      QgramFilter qgramFilter = new QgramFilter ( query._largeQgramGenerator );  
      MergeSkip mergeSkipOperator = new MergeSkip ();
      HashMap< String, Collection< Integer >> approximateSubstringCandidateMap = new HashMap< String, Collection< Integer >>(); 
      
      //implement this function 3
      this.approximatePrefixRangeQuery( query, query._minQgramTokenMatch,
         intersectingNodeStatsMap, approximatePrefixResult,
         approximateSubstringNodeCandidates, doApproximateSubstringNodeFilter, 
         qgramFilter, mergeSkipOperator, approximateSubstringCandidateMap, 
         memObjectMap, memInfreqPosQgramInvertedMap, memInfreqQgramTokenInvertedMap, 
         memPosQgramCountPairInvertedMap, memQgramTokenCountPairInvertedMap, 
         memHilbPosQgramInvertedMap, memHilbQgramTokenInvertedMap );               
                        
      // if there are enough results, return
      this.addResults( resultMap, approximatePrefixResult, memObjectMap, QueryType.APPROXIMATE_PREFIX_RANGE );
      
      stopWatch.approximatePrefixRangeTime += ( System.currentTimeMillis() - approximatePrefixRangeStartTime + intersectingNodeTime );
      
      if ( resultMap.size() >= query.getResultSizeThreshold() )
      {        
        return resultMap;
      }
    }
    
    return resultMap;
  }
  
  
  /*
   * Implement function 1 SP - Spatial prefix query
   */
  public void prefixRangeQuery( 
      SpatialQuery query,
      ArrayList< Integer > prefixResult,
      ArrayList< Integer > prefixResultFromInfrequentQgram,
      ArrayList< Integer > substringCandidates,
      HashMap< String, TreeSet< Integer >> prefixResultMap,
      HashMap< String, TreeSet< Integer >> prefixResultFromNodeLevelJoin,
      TreeMap< String, ArrayList<Integer> > prefixNodeCandidateCountMap,
      TreeMap< String, ArrayList< Integer >> substringNodeCandidateCountMap,
      BooleanObject hasReadInfrequentToken,
      BooleanObject hasInfrequentPosQgram,
      BooleanObject hasInfrequentQgramToken,
      BooleanObject doPrefixNodeFilter,
      BooleanObject doSubstringNodeFilterInPrefixQuery,   
      BooleanObject hasRetrievedIntersectingNodes,
      BooleanObject hasRetrievedIntersectingNodesOfRelaxedRegion,
      IntersectingNodeStatsMap intersectingNodeStatistic,
      IntersectingNodeStatsMap intersectingNodeStatsMapOfRelaxedRegion,
      HashMap< Integer, SpatialObject > visitedObjectMap,
      HashMap< PositionalQgram, TreeSet< Integer >> memInfreqPosQgramInvertedMap,
      HashMap< String, TreeSet< Integer >> memInfreqQgramTokenInvertedMap,
      HashMap< PositionalQgram, HilbertCountMap > memPosQgramCountPairInvertedMap,
      HashMap< String, HilbertCountMap > memQgramTokenCountPairInvertedMap,
      HashMap< SecondLevelKey, TreeSet< Integer >> memHilbPosQgramInvertedMap,
      HashMap< String, TreeSet< Integer >> memHilbQgramTokenInvertedMap )
  {
    // read infrequent q-gram inverted index
	  this.readInfrequentInvertedIndex(query, memInfreqPosQgramInvertedMap, memInfreqQgramTokenInvertedMap, 
			  hasReadInfrequentToken, hasInfrequentPosQgram, hasInfrequentQgramToken);
	 
    
     // if there are infrequent q-grams, join the infrequent inverted list
    
    if ( hasInfrequentPosQgram.isTrue() || hasInfrequentQgramToken.isTrue() )
    {
    	NavigableSet<Integer> infrequentJoinedResult = null;
    	
      // if has infrequent positional q-gram, 
    	if(hasInfrequentPosQgram.isTrue())
    	{
    		infrequentJoinedResult = this.infrequentQgramExactJoin(memInfreqPosQgramInvertedMap.values());
    	}
      // else if has the infrequent q-gram token,
    	else if(hasInfrequentQgramToken.isTrue())
    	{
    		infrequentJoinedResult = this.infrequentQgramExactJoin(memInfreqQgramTokenInvertedMap.values());
    	}
      // check the join result
    	if(infrequentJoinedResult != null)
    	{
    		this.browsePrefixInfrequentResult(query, infrequentJoinedResult, objectDatabase, hasReadInfrequentToken, 
    				prefixResult, visitedObjectMap, prefixResultFromInfrequentQgram, substringCandidates);
    	}

    }
    
    
     // if there is no infrequent q-grams
    else
    {
      // get the intersecting node statistics    
    	//2 types of argument parameters
    	//quadtree.getIntersectingNodeStatistic(readBefore, region, intersectingNodeStatsMap, underLoadThreshold);
    	quadtree.getIntersectingNodeStatistic(query._queryRegion, intersectingNodeStatistic, 
    			sparseThreshold);
    	hasRetrievedIntersectingNodes.setTrue();
    	
      // if the number of dense node is greater than the nodeNumberThreshold, do the prefix node filter node first
      if ( intersectingNodeStatistic.denseNodeNumber >= visitingNodeSizeThreshold )
      {
        // prefix node filter
    	  readIntersectingNodeStatsOfRelaxedRegion(query, hasRetrievedIntersectingNodesOfRelaxedRegion, intersectingNodeStatsMapOfRelaxedRegion);
    	  //2 Arguments
    
    	  prefixNodeFilter(query, intersectingNodeStatsMapOfRelaxedRegion, substringNodeCandidateCountMap, substringNodeCandidateCountMap);
    	  
    	  if(prefixNodeCandidateCountMap.isEmpty())
    	  {
    		  return;
    	  }
    	  else
    	  {
    		  Iterator<Entry<String, ArrayList<Integer>>> prefixNodeItr = prefixNodeCandidateCountMap.entrySet().iterator();
    		  
    		  while(prefixNodeItr.hasNext())
    		  {
    			  Entry<String, ArrayList<Integer>> entry = prefixNodeItr.next();
    			  String nodeHilbertCode = entry.getKey();
    			  NodeStatistic nodeStats = intersectingNodeStatistic.get(nodeHilbertCode);
    			  
    			  if(nodeStats != null)
    			  {
    				//2 types of argument parameters
    				  this.prefixSingleNodeProcess(query, nodeHilbertCode, nodeStats, prefixResult, 
    						  visitedObjectMap, prefixResultMap, 
    						  prefixResultFromNodeLevelJoin, memHilbPosQgramInvertedMap, memHilbQgramTokenInvertedMap);
    			  }
    		  }
    	  }
      }

      // if the number of dense node is not greater than the nodeNumberThreshold,
      // no need to do the prefix node join, directly visit each node
      else
      {
    	  Iterator<Entry<String, NodeStatistic>> intersectingNodeItr = intersectingNodeStatistic.entrySet().iterator();
    	  
    	  while(intersectingNodeItr.hasNext())
    	  {
    		  Entry<String, NodeStatistic> entry = intersectingNodeItr.next();
    		  String nodeHilbertCode = entry.getKey();
    		  NodeStatistic nodeStats = entry.getValue();
    		  //2 types of parameters
    		  this.prefixSingleNodeProcess(query, nodeHilbertCode, nodeStats, prefixResult, visitedObjectMap,
    				  prefixResultMap, prefixResultFromNodeLevelJoin, memHilbPosQgramInvertedMap, memHilbQgramTokenInvertedMap);
    		  
    	  }
      }
    }
  }
  /*
   * Implement function 2 SAP - substring query
   */
  public void substringRangeQuery( 
	      SpatialQuery query, 
	      HashSet< Integer > substringResult,
	      TreeMap< String, ArrayList< Integer >> substringNodeCandidateMap,
	      IntersectingNodeStatsMap intersectingNodeStatsMap,
	      BooleanObject doSubstringNodeFilterInPrefixQuery,
	      BooleanObject doSubstringNodeFilter,
	      boolean intesectingNodeJoin,
	      HashMap< Integer, SpatialObject > memObjectMap,
	      HashMap< String, TreeSet< Integer >> memHilbQgramTokenInvertedMap)
	  { 
	    // if the number of intersecting node is small, do not need to do node filter.
	    if ( intersectingNodeStatsMap.denseNodeNumber < visitingNodeSizeThreshold )
	    {
	      // if the node is never filtered before
	      if ( doSubstringNodeFilterInPrefixQuery.isFalse() && doSubstringNodeFilter.isFalse() )
	      {
	    	  Iterator<Entry<String, NodeStatistic>> itr = intersectingNodeStatsMap.entrySet().iterator();
	    	  while(itr.hasNext())
	    	  {
	    		  Entry<String, NodeStatistic> entry = itr.next();
	    		  String nodeHilbertCode = entry.getKey();
	    		  NodeStatistic nodeStats = entry.getValue();
	    		  
	    		  //substring query in node
	    		//2 types of argument parameters
	    		  //this.substringQueryInSingleNode(query, nodeHilbertCode, nodeStats, resultMap);
	    		  this.substringQueryInSingleNode(query, nodeHilbertCode, nodeStats, substringResult, 
	    				  memObjectMap, memHilbQgramTokenInvertedMap);
	    	
	    	  }
	      }
	      // if the node is filtered before
	      else 
	      {
	        Iterator<Entry<String, ArrayList<Integer>>> nodeItr = substringNodeCandidateMap.entrySet().iterator();
	        while(nodeItr.hasNext())
	        {
	        	Entry<String, ArrayList<Integer>> entry = nodeItr.next();
	        	String nodeHilbertCode = entry.getKey();
	        	NodeStatistic nodeStats = intersectingNodeStatsMap.get(nodeHilbertCode);
	        	
	        	if(nodeStats != null)
	        	{
	        		//2 types of argument parameters
	        		this.substringQueryInSingleNode(query, nodeHilbertCode, nodeStats, substringResult, memObjectMap,
	        				memHilbQgramTokenInvertedMap);
	        	}
	        }
	      }          
	    }
	    
	    // if the number of intersecting node is large, need to do node filter.
	    else
	    {
	      // newly add for the no query reuse case
	      if ( substringNodeCandidateMap == null )
	      {
	        // compute the substringNodeCandidateMap 
	    	  substringNodeCandidateMap = new TreeMap<String, ArrayList<Integer>>();
	    	  this.substringNodeFilter(query, new BooleanObject(false), new BooleanObject(false), 
	    			  new HashMap<String, HilbertCountMap>(), intersectingNodeStatsMap, 
	    		  substringNodeCandidateMap);
	      }    
	      
	      // join the substring node candidate with the intersectingNodeStatsMap
	      if ( ! intesectingNodeJoin )
	      {
	    	  substringIntersectingNodeFilter(substringNodeCandidateMap, intersectingNodeStatsMap);
	      }
	      
	      Iterator< Entry< String, ArrayList< Integer >>> nodeItr = substringNodeCandidateMap.entrySet().iterator();
	      
	      while(nodeItr.hasNext())
	      {
	    	  Entry<String, ArrayList<Integer>> entry = nodeItr.next();
	    	  String nodeHilbertCode = entry.getKey();
	    	  NodeStatistic nodeStats = intersectingNodeStatsMap.get(nodeHilbertCode);
	    	  
	    	//2 types of argument parameters
	    	  //this.substringQueryInSingleNode(query, nodeHilbertCode, nodeStats, resultMap);
	    	  this.substringQueryInSingleNode(query, nodeHilbertCode, nodeStats, 
	    			  substringResult, memObjectMap, memHilbQgramTokenInvertedMap);
	      }
	    } 
	  }
	  
  /*
   * Implement function 3 SAS - approximate prefix
   */
  public void approximatePrefixRangeQuery( SpatialQuery query,
	      int tokenMinMatchThreshold,
	      IntersectingNodeStatsMap intersectingNodeStatsMap,
	      HashSet< Integer > approximatePrefixResult,     
	      TreeSet< String > approximateSubstringNodeCandidates,
	      BooleanObject doApproximateSubstringNodeFilter,
	      QgramFilter qgramFilter,    
	      MergeSkip mergeSkipOperator,
	      HashMap< String, Collection< Integer >> approximateSubstringCandidateMap,
	      HashMap< Integer, SpatialObject > memObjectMap,
	      HashMap< PositionalQgram, TreeSet< Integer >> memInfreqPosQgramInvertedMap,
	      HashMap< String, TreeSet< Integer >> memInfreqQgramTokenInvertedMap,
	      HashMap< PositionalQgram, HilbertCountMap > memPosQgramCountPairInvertedMap,
	      HashMap< String, HilbertCountMap > memQgramTokenCountPairInvertedMap,
	      HashMap< SecondLevelKey, TreeSet< Integer >> memHilbPosQgramInvertedMap,
	      HashMap< String, TreeSet< Integer >> memHilbQgramTokenInvertedMap
	      )
	  {
	  	
	  	NodeStatistic nodeStats = null;
	    // do not need to do node filter
	    if ( intersectingNodeStatsMap.denseNodeNumber < visitingNodeSizeThreshold )
	    {
	      Iterator< Entry< String, NodeStatistic >> itr = intersectingNodeStatsMap.entrySet().iterator();      
	     	       	        
	      while(itr.hasNext())
    	  {
    		  Entry<String, NodeStatistic> entry = itr.next();
    		  String nodeHilbertCode = entry.getKey();
    		  nodeStats = entry.getValue();
    		  
    		  //substring query in node
    		//2 types of argument parameters
    		  if(nodeStats != null)
	        	{
    		  this.approximatePrefixQueryInSingleNode(query, approximatePrefixResult, nodeHilbertCode, nodeStats, memObjectMap, memInfreqPosQgramInvertedMap, memInfreqQgramTokenInvertedMap, memHilbPosQgramInvertedMap,
    				  memHilbQgramTokenInvertedMap, qgramFilter, mergeSkipOperator, approximateSubstringCandidateMap);
	        	}
    	  }
	        // approximate prefix query in node	      
	    }
	    
	    // need to do node filterskenlton
	    else
	    {
	      TreeSet< String > approximatePrefixNodeCandidates = new TreeSet< String >();
	      
	      // node filter 2 filters
	      //this.approximatePrefixNodeFilter(query, tokenMinMatchThreshold, intersectingNodeStatsMap, approximatePrefixNodeCandidates);
	      this.approximatePrefixNodeFilter(query, tokenMinMatchThreshold, intersectingNodeStatsMap, doApproximateSubstringNodeFilter, approximatePrefixNodeCandidates, 
	    		  approximateSubstringNodeCandidates, memPosQgramCountPairInvertedMap, memQgramTokenCountPairInvertedMap);
	      // object process in node       
	      if ( ! approximatePrefixNodeCandidates.isEmpty() )
	      {
	        for ( String nodeHilbertCode : approximatePrefixNodeCandidates )
	        {
	        	this.approximatePrefixQueryInSingleNode(query, approximatePrefixResult, nodeHilbertCode, nodeStats, memObjectMap, memInfreqPosQgramInvertedMap, memInfreqQgramTokenInvertedMap, memHilbPosQgramInvertedMap,
	        			memHilbQgramTokenInvertedMap, qgramFilter, mergeSkipOperator, approximateSubstringCandidateMap);
	        }
	      }
	    }           
	  }

  protected void readInfrequentInvertedIndex( SpatialQuery query,
      HashMap< PositionalQgram, TreeSet< Integer >> infrequentPosQgramMap,
      HashMap< String, TreeSet< Integer >> infrequentQgramTokenMap, 
      BooleanObject hasReadInfrequentToken,
      BooleanObject hasInfrequentPosQgram, 
      BooleanObject hasInfrequentQgramToken )
  {    
    // read infrequent positional q-gram
    getInfrequentPositionalQgramMapping( infrequentPosQgramMap, query._firstMPositionalQgramSet, hasInfrequentPosQgram );

    // if do not have infrequent positional q-grams, check whether there are infrequent q-gram tokens
    if ( hasInfrequentPosQgram.isFalse() )
    {
      getInfrequentQgramTokenMapping( infrequentQgramTokenMap, query._largeQgramTokenSet, hasReadInfrequentToken, hasInfrequentQgramToken );
    }
  }


  private void getInfrequentPositionalQgramMapping(
      HashMap< PositionalQgram, TreeSet< Integer >> infrequentPosQgramMap,
      Set< PositionalQgram > set, BooleanObject hasInfrequentPosQgram )
  {
    for ( PositionalQgram positionalQgram : set )
    {
      int[] objectIdArray = infrequentPosQgramInvertedIndex.read( positionalQgram );
      if ( objectIdArray != null )
      {
        hasInfrequentPosQgram.setTrue();
        TreeSet< Integer > idSet = convertIntArrayToTreeSet( objectIdArray );
        infrequentPosQgramMap.put( positionalQgram, idSet );
      }
      else
      {
        infrequentPosQgramMap.put( positionalQgram, new TreeSet<Integer>() );
      }
    }
  }


  protected void getInfrequentQgramTokenMapping(
      HashMap< String, TreeSet< Integer >> infrequentQgramTokenMap, 
      Set< String > set,
      BooleanObject hasReadInfrequentToken, 
      BooleanObject hasInfrequentQgramToken )
  {
    hasReadInfrequentToken.setTrue();
    for ( String qgramToken : set )
    {
      int[] objectIdArray = infrequentTokenInvertedIndex.read( qgramToken );
      if ( objectIdArray != null )
      {
        hasInfrequentQgramToken.setTrue();
        TreeSet< Integer > idSet = convertIntArrayToTreeSet( objectIdArray );
        infrequentQgramTokenMap.put( qgramToken, idSet );
      }
      else
      {
        infrequentQgramTokenMap.put( qgramToken, new TreeSet<Integer>() );
      }
    }
  }
  
  protected void getInfrequentQgramTokenMapping(
      HashMap< String, TreeSet< Integer >> infrequentQgramTokenMap, 
      Set< String > set,      
      BooleanObject hasInfrequentQgramToken )
  {    
    for ( String qgramToken : set )
    {
      int[] objectIdArray = infrequentTokenInvertedIndex.read( qgramToken );
      if ( objectIdArray != null )
      {
        hasInfrequentQgramToken.setTrue();
        TreeSet< Integer > idSet = convertIntArrayToTreeSet( objectIdArray );
        infrequentQgramTokenMap.put( qgramToken, idSet );
      }
      else
      {
        infrequentQgramTokenMap.put( qgramToken, new TreeSet< Integer >() );
      }
    }
  }


  protected NavigableSet< Integer > infrequentQgramExactJoin( Collection< TreeSet< Integer >> collection )
  {        
    // join the id list
    ArrayList< NavigableSet< Integer >> joinList = new ArrayList< NavigableSet< Integer >>();
    Iterator< TreeSet< Integer >> itr = collection.iterator();
    while ( itr.hasNext() )
    {
      TreeSet< Integer > set = itr.next();
      if ( ! set.isEmpty() )
      {
        joinList.add( set );
      }
    }
    return SortMergeJoin.join( joinList );
  }

 
  protected void browseSubstringInfrequentResult ( 
      SpatialQuery query,
      Collection< Integer > infrequentResult,
      SpatialObjectDatabase objectDatabase,
      HashSet< Integer > substringResult,
      HashMap< Integer, SpatialObject > visitedObjectMap )
  {    
    for ( int objectid : infrequentResult )
    {
      if ( ! substringResult.contains( objectid ) )
      {
        SpatialObject object = visitedObjectMap.get( objectid );
        
        if ( object == null )
        {
          object = objectDatabase.getSpatialObject( objectid );
          // save to visited map
          visitedObjectMap.put( objectid, object );
        }
        
        // check spatial condition
        if ( query._queryRegion.contains( object.getPoint() ) )
        {
          // check substring condition
          if ( query.isSubstring( object._text ) )
          {
            substringResult.add( objectid );
          }             
        } 
      }
    }
  }
  

  protected HashMap< Integer, SimpleEntry < QueryType, SpatialObject >> getSubstringInfrequentResult( 
      SpatialQuery query,
      Collection< Integer > infrequentResult )
  {
    HashMap< Integer, SimpleEntry < QueryType, SpatialObject >> results = 
        new HashMap< Integer, SimpleEntry < QueryType, SpatialObject >>();
    
    for ( int objectid : infrequentResult )
    {      
        SpatialObject object = objectDatabase.getSpatialObject( objectid );   
        
        // check spatial condition
        if ( query._queryRegion.contains( object.getPoint() ) )
        {
          // check substring condition
          if ( query.isSubstring( object._text ) )
          {
            results.put( objectid, new SimpleEntry<QueryType, SpatialObject>( QueryType.SUBSTRING_RANGE, object ) );
          }
        }
      
    }
    return results;
  }
  
  protected void prefixNodeFilter(
      SpatialQuery query,
      BooleanObject doPrefixNodeFilter,
      HashMap< PositionalQgram, HilbertCountMap > memPosQgramCountPairInvertedMap,
      HashMap< String, HilbertCountMap > memQgramTokenCountPairInvertedMap,      
      BooleanObject doSubstringNodeFilterInPrefixQuery,
      IntersectingNodeStatsMap intersectingNodeStatsMapOfRelaxedRegion,
      TreeMap< String, ArrayList<Integer> > prefixNodeCandidateCountMap,
      TreeMap< String, ArrayList<Integer> > substringNodeCandidateCountMap )
  {
    if ( doPrefixNodeFilter.isTrue() )
    {
      return;
    }
    doPrefixNodeFilter.setTrue();
    
    
    ArrayList< HilbertCountMap > joinList = new ArrayList< HilbertCountMap >();
    IntersectionJoin<Integer, NodeStatistic> intersectionJoin = new IntersectionJoin<Integer, NodeStatistic>(); 
    boolean isFirst = true;    
    
    for ( PositionalQgram positionalQgram : query._firstMPositionalQgramSet )
    {      
      if ( ( positionalQgram._pos % query._smallQValue != 0 ) && 
          ( positionalQgram._pos != query._queryText.length() - query._smallQValue ) )
      {
        continue;
      }
                 
      HilbertCountMap map = firstLevelInvertedIndex.read( positionalQgram );
      if ( map != null )
      {
        
        // if it is the first time, join the hilbert count map with the relaxed region
        if ( isFirst )
        {
          intersectionJoin.join( map._map, intersectingNodeStatsMapOfRelaxedRegion );
          isFirst = false;
        }
        
        if ( map.isEmpty() )
        {
          memPosQgramCountPairInvertedMap.put( positionalQgram, new HilbertCountMap() );
          joinList.clear();
          return;
        }
        else
        {
          memPosQgramCountPairInvertedMap.put( positionalQgram, map );        
          joinList.add( map );
        }
      }
      else
      {
        memPosQgramCountPairInvertedMap.put( positionalQgram, new HilbertCountMap() );
        joinList.clear();
        return;
      }
    }
    
    //  get the prefix join result from the positional q-grams
    NodeLevelJoin joinOperator = new NodeLevelJoin();
    TreeMap<String, ArrayList<Integer>> joinResult = joinOperator.join( joinList );    

    
    if ( joinResult == null )
    {
      return;
    }
    else 
    {
      // if the join result is too large, use the large q-grams to filter nodes      
      if ( joinResult.size() >= visitingNodeSizeThreshold  )
      {                
        // join the last a few q-grams
        if ( query._largeQgramTokenSetFromM != null )
        {
          // substring node filter 
          substringNodeFilterInPrefix( query, doSubstringNodeFilterInPrefixQuery,
            memQgramTokenCountPairInvertedMap, intersectingNodeStatsMapOfRelaxedRegion, substringNodeCandidateCountMap );
          
          if ( substringNodeCandidateCountMap.isEmpty() )
          {
            return;
          }
          
          else
          {
            // join the result
            joinResult.keySet().retainAll( substringNodeCandidateCountMap.keySet() );
            
            //  store the result
            if ( joinResult != null && ! joinResult.isEmpty() )
            {
              insertAllEntry(prefixNodeCandidateCountMap, joinResult );
            }
          }          
        }
        else
        {
          insertAllEntry(prefixNodeCandidateCountMap, joinResult );
        }
      }
      //  store the result
      else
      {
        insertAllEntry(prefixNodeCandidateCountMap, joinResult );
      }
    }
  }
  
  protected void prefixNodeFilter(
      SpatialQuery query,            
      IntersectingNodeStatsMap intersectingNodeStats,
      TreeMap< String, ArrayList<Integer> > prefixNodeCandidateCountMap,
      TreeMap< String, ArrayList<Integer> > substringNodeCandidateCountMap )
  {
            
    ArrayList< HilbertCountMap > joinList = new ArrayList< HilbertCountMap >();
    IntersectionJoin<Integer, NodeStatistic> intersectionJoin = new IntersectionJoin<Integer, NodeStatistic>(); 
    boolean isFirst = true;    
    
    for ( PositionalQgram positionalQgram : query._firstMPositionalQgramSet )
    {      
      if ( ( positionalQgram._pos % query._smallQValue != 0 ) && 
          ( positionalQgram._pos != query._queryText.length() - query._smallQValue ) )
      {
        continue;
      }
                 
      HilbertCountMap map = firstLevelInvertedIndex.read( positionalQgram );
      if ( map != null )
      {        
        // if it is the first time, join the hilbert count map with the query region
        if ( isFirst )
        {
          intersectionJoin.join( map._map, intersectingNodeStats );
          isFirst = false;
        }
        
        if ( map.isEmpty() )
        {          
          joinList.clear();
          return;
        }
        else
        {                 
          joinList.add( map );
        }
      }
      else
      {        
        joinList.clear();
        return;
      }
    }
    
    //  get the prefix join result from the positional q-grams
    NodeLevelJoin joinOperator = new NodeLevelJoin();
    TreeMap<String, ArrayList<Integer>> joinResult = joinOperator.join( joinList );    

    
    if ( joinResult == null )
    {
      return;
    }
    else 
    {
      // if the join result is too large, use the large q-grams to filter nodes      
      if ( joinResult.size() >= visitingNodeSizeThreshold  )
      {                
        // join the last a few q-grams
        if ( query._largeQgramTokenSetFromM != null )
        {
          // substring node filter in prefix
          substringNodeFilterInPrefix( query, intersectingNodeStats, substringNodeCandidateCountMap );
          
          if ( substringNodeCandidateCountMap.isEmpty() )
          {
            return;
          }
          
          else
          {
            // join the result
            joinResult.keySet().retainAll( substringNodeCandidateCountMap.keySet() );
            
            //  store the result
            if ( joinResult != null && ! joinResult.isEmpty() )
            {
              insertAllEntry(prefixNodeCandidateCountMap, joinResult );
            }
          }          
        }
        else
        {
          insertAllEntry(prefixNodeCandidateCountMap, joinResult );
        }
      }
      //  store the result
      else
      {
        insertAllEntry(prefixNodeCandidateCountMap, joinResult );
      }
    }
  }
  
  
  
  protected void prefixNodeFilterInPrefixSelectivity(      
      SpatialQuery query,
      BooleanObject doPrefixNodeFilter,
      HashMap< PositionalQgram, HilbertCountMap > memPosQgramCountPairInvertedMap,
      HashMap< String, HilbertCountMap > memQgramTokenCountPairInvertedMap, 
      BooleanObject hasRetrievedIntersectingNodesOfRelaxedRegion,
      IntersectingNodeStatsMap intersectingNodeStatsMapOfRelaxedRegion,
      TreeMap< String, ArrayList<Integer> > prefixNodeCandidateCountMap )
  {
    // only run once
    if ( doPrefixNodeFilter.isTrue() )
    {
      return;
    }
    doPrefixNodeFilter.setTrue();
    
    
    ArrayList< HilbertCountMap > joinList = new ArrayList< HilbertCountMap >();
    IntersectionJoin<Integer, NodeStatistic> intersectionJoin = new IntersectionJoin<Integer, NodeStatistic>(); 
    
    readIntersectingNodeStatsOfRelaxedRegion( query, hasRetrievedIntersectingNodesOfRelaxedRegion, intersectingNodeStatsMapOfRelaxedRegion );
    
    boolean isFirst = true;    
    
    for ( PositionalQgram positionalQgram : query._firstMPositionalQgramSet )
    {
      if ( (positionalQgram._pos % query._smallQValue != 0) && 
          (positionalQgram._pos != query._queryText.length() - query._smallQValue) )
      {
        continue;
      }
      
      HilbertCountMap map = firstLevelInvertedIndex.read( positionalQgram );
      if ( map != null )
      {
        if ( isFirst )
        {
          intersectionJoin.join( map._map, intersectingNodeStatsMapOfRelaxedRegion );
          isFirst = false;
        }
        if ( map.isEmpty() )
        {
          memPosQgramCountPairInvertedMap.put( positionalQgram, new HilbertCountMap() );
          joinList.clear();
          return; 
        }
        else
        {
          memPosQgramCountPairInvertedMap.put( positionalQgram, map );
          joinList.add( map );
        }
      }
      else
      {
        memPosQgramCountPairInvertedMap.put( positionalQgram, new HilbertCountMap() );
        joinList.clear();
        return;
      }
    }
    
    // get the prefix join result from the positional q-grams
    NodeLevelJoin joinOperator = new NodeLevelJoin();
    TreeMap< String, ArrayList< Integer >> joinResult = joinOperator.join( joinList );

    if ( joinResult != null )
    {
      insertAllEntry(prefixNodeCandidateCountMap, joinResult );
    }
  }
  

  
  protected void prefixSingleNodeProcess( 
      SpatialQuery query, 
      String nodeHilbertCode,
      NodeStatistic nodeStats, 
      ArrayList< Integer > prefixResult,
      HashMap< Integer, SpatialObject > visitedObjectMap,
      HashMap< String, TreeSet< Integer >> prefixResultMap,
      HashMap< String, TreeSet< Integer >> prefixResultFromNodeLevelJoin,
      HashMap< SecondLevelKey, TreeSet< Integer >> memHilbPosQgramInvertedMap,
      HashMap< String, TreeSet< Integer >> memHilbQgramTokenInvertedMap )
  {
    // the number of intersecting objects is smaller than the threshold
    // directly retrieve objects
    if ( nodeStats._numberOfObjects <= sparseThreshold
        || nodeStats._intersectingObjectSet.size() <= sparseThreshold )
    {
      TreeSet< Integer > nodeResult = null;
      for ( int objectId : nodeStats._intersectingObjectSet )
      {
        SpatialObject object = objectDatabase.getSpatialObject( objectId );

        // save to visited object map
        visitedObjectMap.put( objectId, object );

        if ( StringVerification.isPrefix( query._queryText, object._text ) )
        {
          prefixResult.add( objectId );        

          if ( nodeResult == null )
          {
            nodeResult = new TreeSet< Integer >();
          }
          nodeResult.add( objectId );
        }

        if ( nodeResult != null )
        {
          prefixResultMap.put( nodeHilbertCode, nodeResult );
        }
      }
    }

    // dense node
    else
    {
      // second level inverted index join
      this.secondLevelPrefixJoinInQueryRegion( query, nodeHilbertCode, nodeStats, prefixResult,
          visitedObjectMap, prefixResultMap, prefixResultFromNodeLevelJoin,
          memHilbPosQgramInvertedMap, memHilbQgramTokenInvertedMap );
    }
  }
  
  

  protected void prefixSingleNodeProcess( 
      SpatialQuery query,       
      NodeStatistic nodeStats,
      String nodeHilbertCode, 
      HashMap< Integer, SimpleEntry < QueryType, SpatialObject > > resultMap )
  {
    // the number of intersecting objects is smaller than the threshold
    // directly retrieve objects
    if ( nodeStats._numberOfObjects <= sparseThreshold || nodeStats._intersectingObjectSet.size() <= sparseThreshold )
    {      
      for ( int objectId : nodeStats._intersectingObjectSet )
      {
        SpatialObject object = objectDatabase.getSpatialObject( objectId );

        // save to visited object map        
        if ( StringVerification.isPrefix( query._queryText, object._text ) )
        {                    
          resultMap.put( objectId, new SimpleEntry<QueryType, SpatialObject>( QueryType.PREFIX_RANGE, object) );         
        }     
      }
    }
    // for dense node, perform second level inverted index
    else
    {
      // second level inverted index join
      secondLevelPrefixJoinInQueryRegion( query, resultMap, nodeHilbertCode, nodeStats );                
    }
  }

  
  protected void prefixSingleNodeProcessOfRelaxedRegion(
      SpatialQuery query, 
      String nodeHilbertCode,
      NodeStatistic nodeStatsOfRelaxedRegion, 
      HashSet< Integer > resultList,
      HashMap< Integer, SpatialObject > memObjectMap)
  {
    // the number of intersecting objects is smaller than the threshold
    // directly retrieve objects
    if ( nodeStatsOfRelaxedRegion._numberOfObjects <= sparseThreshold
        || nodeStatsOfRelaxedRegion._intersectingObjectSet.size() <= sparseThreshold )
    {
      for ( int objectId : nodeStatsOfRelaxedRegion._intersectingObjectSet )
      {
        SpatialObject object = objectDatabase.getSpatialObject( objectId );

        if ( StringVerification.isPrefix( query._queryText, object._text ) )
        {
          resultList.add( objectId );        
        }    
      }
    }

    // dense node
    else
    {
      // second level inverted index join
      this.secondLevelPrefixJoinOutQueryRegion( query, nodeHilbertCode, nodeStatsOfRelaxedRegion, resultList, memObjectMap, false);      
    }
  }


  private void secondLevelPrefixJoinInQueryRegion( 
      SpatialQuery query, 
      String nodeHilbertCode,
      NodeStatistic nodeStats,
      ArrayList< Integer > prefixResult,
      HashMap< Integer, SpatialObject > visitedObjectMap,
      HashMap< String, TreeSet< Integer >> prefixResultMap,
      HashMap< String, TreeSet< Integer >> prefixResultFromNodeLevelJoin,
      HashMap< SecondLevelKey, TreeSet< Integer >> memHilbPosQgramInvertedMap,
      HashMap< String, TreeSet< Integer >> memHilbQgramTokenInvertedMap )
  {
    // get the object list for each gram in the second level inverted index
    ArrayList< NavigableSet< Integer >> invertedObjectJoinVector = new ArrayList< NavigableSet< Integer >>();

    for ( PositionalQgram positionalQgram : query._firstMPositionalQgramSet )
    {
      if ( (positionalQgram._pos % query._smallQValue != 0) && 
          (positionalQgram._pos != query._queryText.length() - query._smallQValue) )
      {
        continue;
      }
      
      int[] objectIdList = secondLevelInvertedIndex.readObjectList( nodeHilbertCode, positionalQgram );

      if ( objectIdList != null )
      {
        TreeSet< Integer > objectList = convertIntArrayToTreeSet( objectIdList );
        invertedObjectJoinVector.add( objectList );

        // save it in memory
        memHilbPosQgramInvertedMap.put( new SecondLevelKey( nodeHilbertCode, positionalQgram ), objectList );
      }
      // the node does not have the positional q-gram, visit next leaf node.
      else
      {
        memHilbPosQgramInvertedMap.put( new SecondLevelKey( nodeHilbertCode, positionalQgram ), new TreeSet< Integer >() );   
        
        // no result, return
        invertedObjectJoinVector.clear();
        prefixResultFromNodeLevelJoin.put( nodeHilbertCode, new TreeSet< Integer >() );
        return;
      }
    }        
    
    // join the inverted list, get the object id of the node    
    TreeSet< Integer > nodeResult = SortMergeJoin.join( invertedObjectJoinVector );
    if ( nodeResult == null )
    {       
      prefixResultFromNodeLevelJoin.put( nodeHilbertCode, new TreeSet< Integer >() );     
      return;
    }
    else
    {
      // the size is large, further join with the inverted list of the q-gram token
      if ( nodeResult.size() > sparseThreshold )
      {               
        // the large q-gram token form lamda
        if ( query._largeQgramTokenSetFromM != null )
        {
          invertedObjectJoinVector.clear();
          invertedObjectJoinVector.add( nodeResult );
          
          for ( String token : query._largeQgramTokenSetFromM )
          {
            String key = nodeHilbertCode + "," + token;
            int[] objectIdList = hilbertQgramTokenInvertedIndex.read( key );

            if ( objectIdList != null )
            {
              TreeSet< Integer > objectList = convertIntArrayToTreeSet( objectIdList );
              invertedObjectJoinVector.add( objectList );

              // save it in memory
              memHilbQgramTokenInvertedMap.put( key, objectList );
            }
            // the node does not have the positional q-gram, visit next leaf node.
            else
            {
              memHilbQgramTokenInvertedMap.put( key, new TreeSet< Integer >() );
              
              // no result, return
              invertedObjectJoinVector.clear();
              prefixResultFromNodeLevelJoin.put( nodeHilbertCode, new TreeSet< Integer >() );
              return;
            }
          }
          
          // join the result again
          nodeResult = SortMergeJoin.join( invertedObjectJoinVector );
          if ( nodeResult == null )
          {       
            prefixResultFromNodeLevelJoin.put( nodeHilbertCode, new TreeSet< Integer >() );      
            return;
          }                    
        }
      }        
      // else, do need to join
    }
    

    // if the algorithm reaches here, it pass the join
    // if the query text is long, need to verify the prefix condition
    if ( query._largeQgramTokenSetFromM != null )
    {
      Iterator< Integer > itr = nodeResult.iterator();
      while ( itr.hasNext() )
      {
        int objectid = itr.next();
        SpatialObject object = objectDatabase.getSpatialObject( objectid );
        visitedObjectMap.put( objectid, object );

        // if it is not a prefix, remove it
        if ( ! StringVerification.isPrefix( query._queryText, object._text ) )
        {
          itr.remove();
        }
      }
    }
    
    // save the join result
    prefixResultFromNodeLevelJoin.put( nodeHilbertCode, nodeResult );

    // check the spatial condition
    // query region contains all objects
    if ( (nodeStats._intersectingRatio >= 1 - Double.MIN_VALUE && nodeStats._intersectingRatio <= 1 + Double.MIN_VALUE) )
    {
      prefixResult.addAll( nodeResult );
      prefixResultMap.put( nodeHilbertCode, nodeResult );       
    }

    // query region contains part of the objects, need to check the spatial condition
    // store the intersecting region
    else
    {
      TreeSet< Integer > intersectingResult = new TreeSet< Integer > ();
      for ( int objectid : nodeResult )
      {
        if ( nodeStats._intersectingObjectSet.contains( objectid ) )
        {
          prefixResult.add( objectid );
          intersectingResult.add( objectid );
        }
      }
      if ( ! intersectingResult.isEmpty() )
      {
        prefixResultMap.put( nodeHilbertCode, intersectingResult );           
      }
   }            
  }
  
  

  private void secondLevelPrefixJoinInQueryRegion( 
      SpatialQuery query, 
      HashMap< Integer, SimpleEntry < QueryType, SpatialObject > > resultMap,
      String nodeHilbertCode,
      NodeStatistic nodeStats )
  {
    // get the object list for each gram in the second level inverted index
    ArrayList< NavigableSet< Integer >> invertedObjectJoinVector = new ArrayList< NavigableSet< Integer >>();

    for ( PositionalQgram positionalQgram : query._firstMPositionalQgramSet )
    {
      if ( (positionalQgram._pos % query._smallQValue != 0) && 
          (positionalQgram._pos != query._queryText.length() - query._smallQValue) )
      {
        continue;
      }
      
      int[] objectIdList = secondLevelInvertedIndex.readObjectList( nodeHilbertCode, positionalQgram );

      if ( objectIdList != null )
      {
        TreeSet< Integer > objectList = convertIntArrayToTreeSet( objectIdList );
        invertedObjectJoinVector.add( objectList );
      }
      // the node does not have the positional q-gram, visit next leaf node.
      else
      {          
        // no result, return
        invertedObjectJoinVector.clear();        
        return;
      }
    }        
    
    // join the inverted list, get the object id of the node    
    TreeSet< Integer > nodeResult = SortMergeJoin.join( invertedObjectJoinVector );
    if ( nodeResult == null )
    {                  
      return;
    }
    else
    {
      // the size is large, further join with the inverted list of the q-gram token
      if ( nodeResult.size() > sparseThreshold )
      {               
        // the large q-gram token form lamda
        if ( query._largeQgramTokenSetFromM != null )
        {
          invertedObjectJoinVector.clear();
          invertedObjectJoinVector.add( nodeResult );
          
          for ( String token : query._largeQgramTokenSetFromM )
          {
            String key = nodeHilbertCode + "," + token;
            int[] objectIdList = hilbertQgramTokenInvertedIndex.read( key );

            if ( objectIdList != null )
            {
              TreeSet< Integer > objectList = convertIntArrayToTreeSet( objectIdList );
              invertedObjectJoinVector.add( objectList );          
            }
            else
            {             
              // no result, return
              invertedObjectJoinVector.clear();             
              return;
            }
          }          
          // join the result again
          nodeResult = SortMergeJoin.join( invertedObjectJoinVector );
          if ( nodeResult == null )
          {                         
            return;
          }                    
        }
      }        
    }
    
    // whether need to check the spatial condition
    boolean nodeContainsAllObjects = false;
    if ( (nodeStats._intersectingRatio >= 1 - Double.MIN_VALUE && nodeStats._intersectingRatio <= 1 + Double.MIN_VALUE) )
    {
      nodeContainsAllObjects = true;
    }       
    
    for ( int objectid : nodeResult )
    {      
      SpatialObject object = objectDatabase.getSpatialObject( objectid );
    
      // check spatial condition
      if ( ! nodeContainsAllObjects )
      {
        if ( ! nodeStats._intersectingObjectSet.contains( objectid ) )
        {
          continue;
        }
      }
      
      // if the query text is long, need to verify the prefix condition
      if ( query._largeQgramTokenSetFromM != null )
      {
        if ( ! StringVerification.isPrefix( query._queryText, object._text ) )
        {
          continue;
        }
      }
      
      // add results
      resultMap.put( objectid, new SimpleEntry<QueryType, SpatialObject >( QueryType.PREFIX_RANGE, object ) );      
    }                          
    
  }
  
  
  public void getPrefixResultFromPreviousComputation(
      HashSet<Integer> prefixAllResult,
      HashMap< String, TreeSet< Integer >> prefixResultMap,
      HashMap< String, TreeSet< Integer >> prefixResultFromNodeLevelJoin,
      IntersectingNodeStatsMap intersectingNodeStatsMap,
      IntersectingNodeStatsMap intersectingNodeStatsMapOfRelaxedRegion )
  {
    // check the visited result first, since it does not need to recompute
    Iterator< Entry< String, NodeStatistic >> intersectingItr = intersectingNodeStatsMap.entrySet().iterator();

    while ( intersectingItr.hasNext() )
    {
      Entry< String, NodeStatistic > entry = intersectingItr.next();
      String hilbertCode = entry.getKey();
      NodeStatistic nodeStats = entry.getValue();

      // directly retrieve
      if ( nodeStats._numberOfObjects <= sparseThreshold
          || nodeStats._intersectingObjectSet.size() <= sparseThreshold )
      {
        NavigableSet< Integer > prefixResultSet = prefixResultMap.get( hilbertCode );
        if ( prefixResultSet != null )
        {
          prefixAllResult.addAll( prefixResultSet );        
        }
      }
      // saved in the prefixResultSet
      else if ( nodeStats._intersectingRatio >= 1 - Double.MIN_VALUE
          && nodeStats._intersectingRatio <= 1 + Double.MIN_VALUE )
      {
        NavigableSet< Integer > prefixResultSet = prefixResultMap.get( hilbertCode );
        if ( prefixResultSet != null )
        {
          prefixAllResult.addAll( prefixResultSet );
        }
      }
      else
      {
        TreeSet< Integer > prefixResultSet = prefixResultFromNodeLevelJoin.get( hilbertCode );
        if ( prefixResultSet != null )
        {
          NodeStatistic nodeStatsOfRelaxedRegion =
              intersectingNodeStatsMapOfRelaxedRegion.get( hilbertCode );

          if ( nodeStatsOfRelaxedRegion._nonIntersectingObjectSet != null )
          {
            prefixResultSet.removeAll( nodeStatsOfRelaxedRegion._nonIntersectingObjectSet );
          }
          prefixAllResult.addAll( prefixResultSet );

        }
      }
    }
  }

  
  
  public boolean allNodeContainScarcePosQgram(
      IntersectingNodeStatsMap intersectingNodeStatsMapOfRelaxedRegion,
      TreeMap< String, ArrayList< Integer > > prefixNodeCandidateCountMap )
  {    
    boolean flag = true;
    
    // intersecting join with the intersectingNodeStatisticMap          
    Iterator< Entry< String, NodeStatistic > > intersectingNodeItr = intersectingNodeStatsMapOfRelaxedRegion.entrySet().iterator();          
    Iterator< Entry< String, ArrayList<Integer> > > prefixNodeItr = prefixNodeCandidateCountMap.entrySet().iterator();
              
    Entry< String, NodeStatistic > intersectEntry = null;
    Entry< String, ArrayList<Integer> > prefixEntry = null;          
    String intersectNodeCode = null;
    String prefixNodeCode = null;         
    NodeStatistic nodeStats = null;
    ArrayList<Integer> posCountList = null;
    
    int comparedValue = 0;   // value equals intersectNodeCode.compareTo( prefixNodeCode );
    
    while ( true )
    {                        
      // get the next key
      
      // both move
      if ( comparedValue == 0 )
      {
        if ( intersectingNodeItr.hasNext() && prefixNodeItr.hasNext() )
        {
          intersectEntry = intersectingNodeItr.next();
          intersectNodeCode = intersectEntry.getKey();
          
          prefixEntry = prefixNodeItr.next();
          prefixNodeCode = prefixEntry.getKey();                               
        }
        else
        {
          break;
        }
      }
      // intersectingNodeItr moves
      else if ( comparedValue < 0 )
      {
        if ( intersectingNodeItr.hasNext() )
        {
          intersectEntry = intersectingNodeItr.next();
          intersectNodeCode = intersectEntry.getKey();
        }
        else
        {
          break;
        }
      }
      // prefixNodeItr moves
      else
      {
        if ( prefixNodeItr.hasNext() )
        {
          prefixEntry = prefixNodeItr.next();
          prefixNodeCode = prefixEntry.getKey();
        }
        else
        {
          break;
        }
      }
      
      // compare the key
      comparedValue = intersectNodeCode.compareTo( prefixNodeCode );
      if ( comparedValue == 0 )
      {
        nodeStats = intersectEntry.getValue();
        posCountList = prefixEntry.getValue();
        
        int posCountMin = getMinimumValue( posCountList );
        
        if ( ( posCountMin / (nodeStats._numberOfObjects * 1.0) ) >= scarceThreshold )
        {
          return false;
        } 
      }      
    }
    
    return flag;
  }

  
  
  public double prefixSelectivityInRelaxedRegion( 
      SpatialQuery query, 
      int prefixSelectivityFromPreviousComputation,
      BooleanObject doPrefixNodeFilter,
      BooleanObject doSubstringNodeFilterInPrefixQuery, 
      BooleanObject doSubstringNodeFiler,
      BooleanObject hasReadSmallQgram,
      BooleanObject hasRetrievedIntersectingNodesOfRelaxedRegion,
      HashMap< String, TreeSet< Integer >> prefixResultMap,
      IntersectingNodeStatsMap intersectingNodeStatsMap,
      IntersectingNodeStatsMap intersectingNodeStatsMapOfRelaxedRegion,
      HashMap< PositionalQgram, HilbertCountMap > memPosQgramCountPairInvertedMap,
      HashMap< String, HilbertCountMap > memQgramTokenCountPairInvertedMap,
      HashMap< SecondLevelKey, TreeSet< Integer >> memHilbPosQgramInvertedMap,
      HashMap< String, TreeSet< Integer >> memHilbQgramTokenInvertedMap,
      TreeMap< String, ArrayList< Integer > > prefixNodeCandidateCountMap,
      TreeMap< String, ArrayList< Integer > > substringNodeCandidateCountMap,
      HashMap< String, Double> substringSelectivityMap)
  {
    double selectivity = prefixSelectivityFromPreviousComputation;
    query._queryType = QueryType.PREFIX_SELECTIVITY;
       
    int intersectingNodeSizeDifference = intersectingNodeStatsMapOfRelaxedRegion.size() - intersectingNodeStatsMap.size();

    // situations when the prefix node filter is needed to apply
    if ( ( intersectingNodeStatsMap.denseNodeNumber >= visitingNodeSizeThreshold ) || 
         ( intersectingNodeSizeDifference >= visitingNodeSizeThreshold ) )
    {      
      // do the prefix node filtering                
      prefixNodeFilterInPrefixSelectivity( query, doPrefixNodeFilter, memPosQgramCountPairInvertedMap, memQgramTokenCountPairInvertedMap, 
          hasRetrievedIntersectingNodesOfRelaxedRegion,intersectingNodeStatsMapOfRelaxedRegion, prefixNodeCandidateCountMap );
                     
      if ( prefixNodeCandidateCountMap.isEmpty() )
      {
        return 0;
      }      
            
      boolean allNodeContainScarcePosQgram = this.allNodeContainScarcePosQgram( intersectingNodeStatsMapOfRelaxedRegion, prefixNodeCandidateCountMap );
      
      // if all the node contain a scarce positional q-gram, do not need to do substring node filter
      if ( ! allNodeContainScarcePosQgram )
      {
        if ( query._largeQgramTokenSet != null )    
        {
          // join the result with the substring node candidate
          substringNodeFilter( query, doSubstringNodeFilterInPrefixQuery, doSubstringNodeFiler,
              memQgramTokenCountPairInvertedMap, intersectingNodeStatsMapOfRelaxedRegion, substringNodeCandidateCountMap );
  
          IntersectionJoin< ArrayList< Integer >, ArrayList< Integer >> joinOperator = new IntersectionJoin< ArrayList< Integer >, ArrayList< Integer >>();
          joinOperator.join( prefixNodeCandidateCountMap, substringNodeCandidateCountMap );
        }
      }           
           
      if ( ! prefixNodeCandidateCountMap.isEmpty() )
      {
        PositionalQgram firstPosQgram = new PositionalQgram( query._queryText.substring( 0, query._smallQValue ), 0 );
        HilbertCountMap firstPosQgramHilbertCountMap = memPosQgramCountPairInvertedMap.get( firstPosQgram );
  
        Iterator<Entry<String, ArrayList<Integer>>> prefixNodeItr = prefixNodeCandidateCountMap.entrySet().iterator();
        // check each candidate node
        while ( prefixNodeItr.hasNext() )
        {      
          Entry<String, ArrayList<Integer>> prefixNodeEntry = prefixNodeItr.next();
          String nodeHilbertCode = prefixNodeEntry.getKey();
          ArrayList< Integer > posQgramCountList = prefixNodeEntry.getValue();        
          
          NodeStatistic visitedNodeStats = intersectingNodeStatsMap.get(nodeHilbertCode);        
          // if the node is visited before
          if ( visitedNodeStats != null )
          {                
            // only need to compute the additional objects for the small intersection node
            if ( visitedNodeStats._numberOfObjects <= sparseThreshold
                || visitedNodeStats._intersectingObjectSet.size() <= sparseThreshold )
            {
              if ( visitedNodeStats._nonIntersectingObjectSet != null && ! visitedNodeStats._nonIntersectingObjectSet.isEmpty() )
              {
                TreeSet<Integer> previousResult = prefixResultMap.get( nodeHilbertCode );
                int previousResultSize = 0;
                if ( previousResult != null )
                {
                  previousResultSize = previousResult.size();
                }                
                              
                double nodeSelectivity = this.getPrefixSelectivityOfSingleNode( 
                  query, allNodeContainScarcePosQgram, hasReadSmallQgram, nodeHilbertCode,
                  visitedNodeStats._numberOfObjects, firstPosQgramHilbertCountMap, 
                  posQgramCountList, substringNodeCandidateCountMap, 
                  memQgramTokenCountPairInvertedMap, substringSelectivityMap );                
                
                // newly add
                NodeStatistic nodeStatsOfRelaxedRegion = intersectingNodeStatsMapOfRelaxedRegion.get( nodeHilbertCode );
                nodeSelectivity *= nodeStatsOfRelaxedRegion._intersectingRatio;
                                              
                // add the difference value
                if ( nodeSelectivity > previousResultSize )
                {
                  selectivity += ( nodeSelectivity - previousResultSize );                  
                }                               
              }
            }
          }
          // if the node is never visited before
          else 
          {                                      
            NodeStatistic nodeStatsOfRelaxedRegion = intersectingNodeStatsMapOfRelaxedRegion.get( nodeHilbertCode );
  
            double nodeSelectivity = this.getPrefixSelectivityOfSingleNode( 
                    query, allNodeContainScarcePosQgram,
                    hasReadSmallQgram, nodeHilbertCode, 
                    nodeStatsOfRelaxedRegion._numberOfObjects, firstPosQgramHilbertCountMap,
                    posQgramCountList, substringNodeCandidateCountMap,
                    memQgramTokenCountPairInvertedMap, substringSelectivityMap );
  
            selectivity += nodeSelectivity * nodeStatsOfRelaxedRegion._intersectingRatio;                           
          }       
        }                          
      }      
    }
    
    // the size of the intersecting node is small, do not need to do prefix node filter
    else
    {      
      // read each intersecting node
      Iterator<Entry<String, NodeStatistic>> nodeItr = intersectingNodeStatsMapOfRelaxedRegion.entrySet().iterator();            
      while ( nodeItr.hasNext() )
      {      
        Entry<String, NodeStatistic> nodeEntry = nodeItr.next();
        String nodeHilbertCode = nodeEntry.getKey();
        NodeStatistic nodeStatsOfRelaxedRegion = nodeEntry.getValue();        
        
        NodeStatistic visitedNodeStats = intersectingNodeStatsMap.get(nodeHilbertCode);        
        // if the node is visited before
        if ( visitedNodeStats != null )
        {                
          // only need to compute the additional objects for the small intersection node
          if ( visitedNodeStats._numberOfObjects <= sparseThreshold
              || visitedNodeStats._intersectingObjectSet.size() <= sparseThreshold )
          {
            if ( visitedNodeStats._nonIntersectingObjectSet != null && ! visitedNodeStats._nonIntersectingObjectSet.isEmpty() )
            {
              TreeSet<Integer> previousResult = prefixResultMap.get( nodeHilbertCode );
              int previousResultSize = 0;
              if ( previousResult != null )
              {
                previousResultSize = previousResult.size();
              }              
                            
              double nodeSelectivity = this.getPrefixMaxSelectivityOfSingleNodeInObjectLevel( 
                query, nodeHilbertCode, scarceThreshold, nodeStatsOfRelaxedRegion._numberOfObjects,
                memHilbPosQgramInvertedMap, memHilbQgramTokenInvertedMap, substringSelectivityMap );                             
              
              nodeSelectivity *= nodeStatsOfRelaxedRegion._intersectingRatio;                            
              
              // add the difference value
              if ( nodeSelectivity > previousResultSize )
              {
                selectivity += ( nodeSelectivity - previousResultSize );                  
              }             
            }         
          }            
        }
        // if the node is never visited before
        else 
        {                                        
          double nodeSelectivity = this.getPrefixMaxSelectivityOfSingleNodeInObjectLevel( 
              query, nodeHilbertCode, scarceThreshold, nodeStatsOfRelaxedRegion._numberOfObjects,
              memHilbPosQgramInvertedMap, memHilbQgramTokenInvertedMap, substringSelectivityMap ); 

          selectivity += nodeSelectivity * nodeStatsOfRelaxedRegion._intersectingRatio;                        
        }       
      }        
    }       
     if ( selectivity < Double.MIN_VALUE && selectivity > -Double.MIN_VALUE)
    {
      System.out.println( "0 value" );
      System.out.println( "id: " + query.id );      
      System.out.println( "length: " + query._queryText.length() );      
    }
    
    return selectivity;
  }

  
  public boolean isPrefixSelectivityInRelaxedRegionExceedSelectivityThreshold(                                                                              
      SpatialQuery query,
      int prefixSelectivityFromPreviousComputation,
      BooleanObject doPrefixNodeFilter, 
      BooleanObject doSubstringNodeFilterInPrefixQuery,
      BooleanObject doSubstringNodeFiler, 
      BooleanObject hasReadSmallQgram,
      BooleanObject hasRetrievedIntersectingNodesOfRelaxedRegion,
      HashMap< String, TreeSet< Integer >> prefixResultMap,
      IntersectingNodeStatsMap intersectingNodeStatsMap,
      IntersectingNodeStatsMap intersectingNodeStatsMapOfRelaxedRegion,
      HashMap< PositionalQgram, HilbertCountMap > memPosQgramCountPairInvertedMap,
      HashMap< String, HilbertCountMap > memQgramTokenCountPairInvertedMap,
      HashMap< SecondLevelKey, TreeSet< Integer >> memHilbPosQgramInvertedMap,
      HashMap< String, TreeSet< Integer >> memHilbQgramTokenInvertedMap,
      TreeMap< String, ArrayList< Integer > > prefixNodeCandidateCountMap,
      TreeMap< String, ArrayList< Integer > > substringNodeCandidateCountMap,
      HashMap< String, Double > substringSelectivityMap )
  {
    double selectivity = prefixSelectivityFromPreviousComputation;
    query._queryType = QueryType.PREFIX_SELECTIVITY;
       
    int intersectingNodeSizeDifference = intersectingNodeStatsMapOfRelaxedRegion.size() - intersectingNodeStatsMap.size();
    
    // situations when the prefix node filter is needed to apply
    if ( ( intersectingNodeStatsMap.denseNodeNumber >= visitingNodeSizeThreshold ) || 
         ( intersectingNodeSizeDifference >= visitingNodeSizeThreshold ) )
    {      
      // do the prefix node filtering                
      prefixNodeFilterInPrefixSelectivity( query, doPrefixNodeFilter, memPosQgramCountPairInvertedMap, memQgramTokenCountPairInvertedMap, 
          hasRetrievedIntersectingNodesOfRelaxedRegion,intersectingNodeStatsMapOfRelaxedRegion, prefixNodeCandidateCountMap );
                     
      if ( prefixNodeCandidateCountMap.isEmpty() )
      {
        return ( 0 >= selectivityThreshold );
      }      
            
      boolean allNodeContainScarcePosQgram = this.allNodeContainScarcePosQgram( intersectingNodeStatsMapOfRelaxedRegion, prefixNodeCandidateCountMap );
      
      // if all the node contain a scarce positional q-gram, do not need to do substring node filter
      if ( ! allNodeContainScarcePosQgram )
      {
        if ( query._largeQgramTokenSet != null )    
        {
          // join the result with the substring node candidate
          substringNodeFilter( query, doSubstringNodeFilterInPrefixQuery, doSubstringNodeFiler,
              memQgramTokenCountPairInvertedMap, intersectingNodeStatsMapOfRelaxedRegion, substringNodeCandidateCountMap );
  
          IntersectionJoin< ArrayList< Integer >, ArrayList< Integer >> joinOperator = new IntersectionJoin< ArrayList< Integer >, ArrayList< Integer >>();
          joinOperator.join( prefixNodeCandidateCountMap, substringNodeCandidateCountMap );
        }
      }
           
      if ( ! prefixNodeCandidateCountMap.isEmpty() )
      {
        PositionalQgram firstPosQgram = new PositionalQgram( query._queryText.substring( 0, query._smallQValue ), 0 );
        HilbertCountMap firstPosQgramHilbertCountMap = memPosQgramCountPairInvertedMap.get( firstPosQgram );
  
        Iterator<Entry<String, ArrayList<Integer>>> prefixNodeItr = prefixNodeCandidateCountMap.entrySet().iterator();
        // check each candidate node
        while ( prefixNodeItr.hasNext() )
        {      
          Entry<String, ArrayList<Integer>> prefixNodeEntry = prefixNodeItr.next();
          String nodeHilbertCode = prefixNodeEntry.getKey();
          ArrayList< Integer > posQgramCountList = prefixNodeEntry.getValue();        
          
          NodeStatistic visitedNodeStats = intersectingNodeStatsMap.get(nodeHilbertCode);
          
          // if the node is visited before
          if ( visitedNodeStats != null )
          {                
            // only need to compute the additional objects for the small intersection node
            if ( visitedNodeStats._numberOfObjects <= sparseThreshold
                || visitedNodeStats._intersectingObjectSet.size() <= sparseThreshold )
            {
              if ( visitedNodeStats._nonIntersectingObjectSet != null && ! visitedNodeStats._nonIntersectingObjectSet.isEmpty() )
              {
                TreeSet<Integer> previousResult = prefixResultMap.get( nodeHilbertCode );
                int previousResultSize = 0;
                if ( previousResult != null )
                {
                  previousResultSize = previousResult.size();
                }                
                              
                double nodeSelectivity = this.getPrefixSelectivityOfSingleNode( 
                  query, allNodeContainScarcePosQgram, hasReadSmallQgram, nodeHilbertCode,
                  visitedNodeStats._numberOfObjects, firstPosQgramHilbertCountMap, 
                  posQgramCountList, substringNodeCandidateCountMap, 
                  memQgramTokenCountPairInvertedMap, substringSelectivityMap );                
                
                // newly add
                NodeStatistic nodeStatsOfRelaxedRegion = intersectingNodeStatsMapOfRelaxedRegion.get( nodeHilbertCode );
                nodeSelectivity *= nodeStatsOfRelaxedRegion._intersectingRatio;
                                              
                // add the difference value
                if ( nodeSelectivity > previousResultSize )
                {
                  selectivity += ( nodeSelectivity - previousResultSize );                  
                } 
              }         
            }            
          }
          // if the node is never visited before
          else 
          {                                      
            NodeStatistic nodeStatsOfRelaxedRegion = intersectingNodeStatsMapOfRelaxedRegion.get( nodeHilbertCode );
  
            double nodeSelectivity = this.getPrefixSelectivityOfSingleNode( 
                    query, allNodeContainScarcePosQgram,
                    hasReadSmallQgram, nodeHilbertCode, 
                    nodeStatsOfRelaxedRegion._numberOfObjects, firstPosQgramHilbertCountMap,
                    posQgramCountList, substringNodeCandidateCountMap,
                    memQgramTokenCountPairInvertedMap, substringSelectivityMap );
  
            selectivity += nodeSelectivity * nodeStatsOfRelaxedRegion._intersectingRatio;                           
          } 
          
          // if the selectivity exceeds the threshold, early stop.
          if ( selectivity >= selectivityThreshold )
          {
            return true;
          }
        }                          
      }      
    }
    
    
    // the intersecting node size is small, do not need to do the prefix node filter
    else
    {      
      // read each intersecting node
      Iterator<Entry<String, NodeStatistic>> nodeItr = intersectingNodeStatsMapOfRelaxedRegion.entrySet().iterator();            
      while ( nodeItr.hasNext() )
      {      
        Entry<String, NodeStatistic> nodeEntry = nodeItr.next();
        String nodeHilbertCode = nodeEntry.getKey();
        NodeStatistic nodeStatsOfRelaxedRegion = nodeEntry.getValue();        
        
        NodeStatistic visitedNodeStats = intersectingNodeStatsMap.get(nodeHilbertCode);        
        // if the node is visited before
        if ( visitedNodeStats != null )
        {                
          // only need to compute the additional objects for the small intersection node
          if ( visitedNodeStats._numberOfObjects <= sparseThreshold
              || visitedNodeStats._intersectingObjectSet.size() <= sparseThreshold )
          {
            if ( visitedNodeStats._nonIntersectingObjectSet != null && ! visitedNodeStats._nonIntersectingObjectSet.isEmpty() )
            {
              TreeSet<Integer> previousResult = prefixResultMap.get( nodeHilbertCode );
              int previousResultSize = 0;
              if ( previousResult != null )
              {
                previousResultSize = previousResult.size();
              }              
                            
              double nodeSelectivity = this.getPrefixMaxSelectivityOfSingleNodeInObjectLevel( 
                query, nodeHilbertCode, scarceThreshold, nodeStatsOfRelaxedRegion._numberOfObjects,
                memHilbPosQgramInvertedMap, memHilbQgramTokenInvertedMap, substringSelectivityMap );                             
              
              nodeSelectivity *= nodeStatsOfRelaxedRegion._intersectingRatio;                            
              
         
            selectivity += Math.max( nodeSelectivity, previousResultSize );
            
            }         
          }            
        }
        // if the node is never visited before
        else 
        {                                        
          double nodeSelectivity = this.getPrefixMaxSelectivityOfSingleNodeInObjectLevel( 
              query, nodeHilbertCode, scarceThreshold, nodeStatsOfRelaxedRegion._numberOfObjects,
              memHilbPosQgramInvertedMap, memHilbQgramTokenInvertedMap, substringSelectivityMap ); 

          selectivity += nodeSelectivity * nodeStatsOfRelaxedRegion._intersectingRatio;                        
        }  
        
        // if the selectivity exceeds the threshold, stop.
        if ( selectivity >= selectivityThreshold )
        {
          return true;
        }        
      }        
    }       
    
    return ( selectivity >= selectivityThreshold );
  }
  
    
  
  
  private double getPrefixSelectivityOfSingleNode( 
      SpatialQuery query, 
      boolean allNodeContainScarcePosQgram,
      BooleanObject hasReadSmallQgram,
      final String hilbertCode, 
      final int objectNumberInNode,
      final HilbertCountMap firstPosQgramHilbertCountMap,
      final ArrayList< Integer > posQgramCountList,
      final TreeMap< String, ArrayList< Integer > > substringNodeCandidateCountMap,
      final HashMap< String, HilbertCountMap > memQgramTokenCountPairInvertedMap, 
      HashMap< String, Double > substringSelectivityMap )
  {
    // All the Node Contain Scarce Positional q-gram 
    if ( allNodeContainScarcePosQgram )
    {
      return getMinimumValue( posQgramCountList );
    }           
    
    double selectivity = 1.0;     
    double prefixRatio = 1.0;
    int normalCountMin = Integer.MAX_VALUE;
    
    /**
     * check whether there are scare q-grams
     */
    int posCountMin = getMinimumValue( posQgramCountList );
    ArrayList< Integer > normalCountList = null;
    
    if ( query._largeQgramTokenSet == null )
    {
      return posCountMin;
    }
    else
    {
      normalCountList = substringNodeCandidateCountMap.get( hilbertCode );
      if ( normalCountList == null )
      {
        System.err.println( "Error in SpatialInstaantQuery.getPrefixSelectivityOfSingleNode() " );
        return -1;
      }
      normalCountMin = getMinimumValue( normalCountList ); 
    }
       
    double minCount = Math.min( posCountMin, normalCountMin );
    
    if ( ( minCount / (objectNumberInNode * 1.0) ) < scarceThreshold )
    {
      selectivity = minCount;
            
      if ( minCount != posCountMin )
      {
        if ( ( normalCountMin / (objectNumberInNode * 1.0) ) < scarceThreshold )
        {
          substringSelectivityMap.put( hilbertCode, normalCountMin * 1.0 );          
        }
          
        if ( ( posCountMin / (objectNumberInNode * 1.0) ) >= scarceThreshold )
        {
         // need to multiply the prefix ratio
          Integer firstPosQgramCount = firstPosQgramHilbertCountMap._map.get( hilbertCode );                   
          String firstQgram = query._queryText.substring( 0, query._smallQValue );
          HilbertCountMap map = memQgramTokenCountPairInvertedMap.get( firstQgram );                            
          if ( map == null )
          {
            map = qgramTokenCountPairInvertedIndex.read( firstQgram );
            memQgramTokenCountPairInvertedMap.put( firstQgram, map );
          }
          
          Integer firstSmallQgramCount = map._map.get( hilbertCode );
            
          if ( firstPosQgramCount == null || firstSmallQgramCount == null )
          {             
            System.err.println( "Error in SpatialInstaantQuery.getPrefixSelectivityOfSingleNode() " );           
            return -1;  
          }                     
          prefixRatio = ( firstPosQgramCount / firstSmallQgramCount * 1.0);
          selectivity *= prefixRatio;
        }        
      }      
      return selectivity;      
    }     
    
    /**
     *  if reach here, there is no scare q-gram
     */      
    
    selectivity = 1.0;
    for ( int numerator : normalCountList )
    {
      selectivity *= (numerator * 1.0);
    }
    
//    String lastSmallQgram = query._queryText.substring( query._queryText.length() - query._smallQValue );

    this.readSmallQgramTokenHilbertCountPair( query, hasReadSmallQgram, memQgramTokenCountPairInvertedMap, substringNodeCandidateCountMap );
    
    for ( String smallToken : query._qgramSetWithoutWildCard )
    {     
      HilbertCountMap map = memQgramTokenCountPairInvertedMap.get( smallToken );
      int denominator = map._map.get( hilbertCode );
      selectivity /= (denominator * 1.0); 
    }
           
    HilbertCountMap map = memQgramTokenCountPairInvertedMap.get( query._queryText.substring( 0, query._smallQValue ) );         
    Integer firstSmallQgramCount = map._map.get( hilbertCode );

    double substringSelectivity = selectivity * firstSmallQgramCount;    
    // store substring selectivity
    substringSelectivityMap.put( hilbertCode, Math.min( substringSelectivity, normalCountMin ) );      
   
    
    Integer firstPosQgramCount = firstPosQgramHilbertCountMap._map.get( hilbertCode );  
    if ( firstPosQgramCount == null )
    {             
      System.err.println( "Error in SpatialInstaantQuery.getPrefixSelectivityOfSingleNode() " );           
      return -1;  
    }   
    double prefixSelectivity = selectivity * firstPosQgramCount;
    
    return Math.min( prefixSelectivity, minCount );     
  }
  
  
  
  /**
   * 
   * @param query
   * @param nodeStatsInRelaxedRegion
   * @param hilbertCode
   * @param scarceThreshold
   * @param objectNumberInNodeInRelaxedRegion
   * @param memHilbPosQgramInvertedMap
   * @param memHilbQgramTokenInvertedMap
   * @param substringSelectivityMap
   * @return the minimal value of the count ( or the maximal bound of selectivity )
   */
  private int getPrefixMaxSelectivityOfSingleNodeInObjectLevel(
      SpatialQuery query,   
      final String hilbertCode,
      final double scarceThreshold,
      final int objectNumberInNodeInRelaxedRegion,
      HashMap< SecondLevelKey, TreeSet< Integer >> memHilbPosQgramInvertedMap,
      HashMap< String, TreeSet< Integer >> memHilbQgramTokenInvertedMap,                 
      HashMap< String, Double > substringSelectivityMap )
  {       
    int minCount = Integer.MAX_VALUE;
    
    // check the objects containing the positional q-gram in the node
    for ( PositionalQgram positionalQgram : query._firstMPositionalQgramSet )
    {
      SecondLevelKey secondLevelKey = new SecondLevelKey( hilbertCode, positionalQgram );
      TreeSet<Integer> idSet = memHilbPosQgramInvertedMap.get( secondLevelKey );
      
      // need to read in the database 
      if ( idSet == null )
      {
        idSet = secondLevelInvertedIndex.readObjectList( secondLevelKey );
        if ( idSet == null )
        {
          memHilbPosQgramInvertedMap.put( secondLevelKey, new TreeSet<Integer>() );
          return 0;
        }
        else
        {
          memHilbPosQgramInvertedMap.put( secondLevelKey, idSet );
          minCount = Math.min( minCount, idSet.size() );
        }
      }
      // the idSet is in memory
      else 
      {
        if ( idSet.isEmpty() )
        {
          return 0;
        }
        else
        {
          minCount = Math.min( minCount, idSet.size() );
        }
      }           
    }
    
    // check whether there is a scarce positional q-gram
    if ( ( minCount / (objectNumberInNodeInRelaxedRegion * 1.0) ) < scarceThreshold )
    {
      return minCount;
    }
    
    
    int minTokenCount = Integer.MAX_VALUE;    
    
    // if there is no scarce positional q-gram, read the q-gram token
    if ( query._largeQgramTokenPositionMap == null )
    {
      return minCount;
    }
    else      
    {
      for( String token : query._largeQgramTokenPositionMap.keySet() )
      {
        String key = hilbertCode + "," + token;
        TreeSet<Integer> idSet = memHilbQgramTokenInvertedMap.get( key );
        
        // read from disk
        if ( idSet == null )
        {
          idSet = hilbertQgramTokenInvertedIndex.getIdSet( key );
          if ( idSet == null )
          {
            memHilbQgramTokenInvertedMap.put( key, new TreeSet<Integer>() );
            minTokenCount = 0;
            break;
          }
          else
          {
            memHilbQgramTokenInvertedMap.put( key, idSet );
            minTokenCount = Math.min( minTokenCount, idSet.size() );
          }
        }
        // already in memory
        else
        {
          if ( idSet.isEmpty() )
          {
            minTokenCount = 0;
            break;
          }
          else
          {
            minTokenCount = Math.min( minTokenCount, idSet.size() );
          }
        }
      }
    }
    
    // if the code reach here, it checks the q-gram tokens, with is the substring max selectivity
    substringSelectivityMap.put( hilbertCode, minTokenCount * 1.0 );
    return Math.min( minCount, minTokenCount);
  }
  
  
   

  public void prefixQueryInRelaxedRegion( 
      SpatialQuery query, 
      HashSet< Integer > prefixResultAll,
      BooleanObject doPrefixNodeFilter,
      TreeMap<String, ArrayList<Integer>> prefixNodeCandidateCountMap,
      HashMap< String, TreeSet< Integer >> prefixResultFromNodeLevelJoin,
      HashMap< Integer, SpatialObject > memObjectMap,
      IntersectingNodeStatsMap intersectingNodeStatsMap,
      IntersectingNodeStatsMap intersectingNodeStatsMapOfRelaxedRegion)
  {      
    // need to handle the non-node-filter case          
    Iterator< Entry< String, NodeStatistic> > itr = intersectingNodeStatsMapOfRelaxedRegion.entrySet().iterator();
            
    while ( itr.hasNext() )
    {
      Entry< String, NodeStatistic > nodeEntry = itr.next();
      String nodeHilbertCode = nodeEntry.getKey();
      NodeStatistic nodeStatsOfRelaxedRegion = nodeEntry.getValue();
            
      
      // if the prefix filtered nodes do not contain the node, skip
      if ( doPrefixNodeFilter.isTrue() )
      {
        if ( ! prefixNodeCandidateCountMap.containsKey( nodeHilbertCode ) )
        {
          continue;
        }
      }      
      
      NodeStatistic nodeStats = intersectingNodeStatsMap.get( nodeHilbertCode );      

      // intersecting nodes
      if ( nodeStats != null )
      {
        // the query range covers all the objects in the node
        if ( nodeStats._intersectingRatio >= 1 - Double.MIN_VALUE
            && nodeStats._intersectingRatio <= 1 + Double.MIN_VALUE )
        {
          // have added the result,
          // skip          
        }
        // the node have been visited before,
        else if ( nodeStats._intersectingObjectSet.size() > sparseThreshold )
        {
          // get the result from the join result
          TreeSet< Integer > result = prefixResultFromNodeLevelJoin.get( nodeHilbertCode );
          if ( result != null )
          {
            // retain the intersecting objects 
            result.retainAll( nodeStatsOfRelaxedRegion._intersectingObjectSet );                       
            prefixResultAll.addAll( result );
          }
        }
        // need to check the non intersecting objects
        else
        {
          nodeStatsOfRelaxedRegion._intersectingObjectSet.retainAll( nodeStats._nonIntersectingObjectSet );
          
          // if there are few objects needed to be checked 
          if ( nodeStatsOfRelaxedRegion._intersectingObjectSet.size() <= sparseThreshold )
          {
            // directly retrieve
            for ( int objectid : nodeStatsOfRelaxedRegion._intersectingObjectSet )
            {
              SpatialObject object = objectDatabase.getSpatialObject( objectid );
              if ( StringVerification.isPrefix( query._queryText, object.getText() ) )
              {
                prefixResultAll.add( objectid );
              }
            }
          }
          else
          {
            // read the second level inverted index and join the result
            this.secondLevelPrefixJoinOutQueryRegion( query, nodeHilbertCode, nodeStatsOfRelaxedRegion, prefixResultAll, memObjectMap, true );
          }
        }
      }
      // for the non-intersecting nodes
      else
      {
        // get the node stats for the relaxed region.
        this.prefixSingleNodeProcessOfRelaxedRegion( query, nodeHilbertCode, nodeStatsOfRelaxedRegion, prefixResultAll, memObjectMap ); 
      }
    }
  }
  
  public void SPRForAppendingQuery( 
      SpatialQuery query, 
      HashSet< Integer > prefixResultAll,
      BooleanObject doPrefixNodeFilter,
      TreeMap< String, ArrayList< Integer >> prefixNodeCandidateCountMap,   
      // HashMap< String, TreeSet< Integer >> prefixResultFromNodeLevelJoin,
      HashMap< Integer, SpatialObject > memObjectMap,
      IntersectingNodeStatsMap intersectingNodeStatsMap,
      IntersectingNodeStatsMap intersectingNodeStatsMapOfRelaxedRegion )
  {
    // need to handle the non-node-filter case
    Iterator< Entry< String, NodeStatistic > > itr =  intersectingNodeStatsMapOfRelaxedRegion.entrySet().iterator();

    while ( itr.hasNext() )
    {
      Entry< String, NodeStatistic > nodeEntry = itr.next();
      String nodeHilbertCode = nodeEntry.getKey();
      NodeStatistic nodeStatsOfRelaxedRegion = nodeEntry.getValue();
      
      // if the prefix filtered nodes do not contain the node, skip
      if ( doPrefixNodeFilter.isTrue() )
      {
        if ( !prefixNodeCandidateCountMap.containsKey( nodeHilbertCode ) )
        {
          continue;
        }
      }
      
      NodeStatistic nodeStats = intersectingNodeStatsMap.get( nodeHilbertCode );

      // intersecting nodes
      if ( nodeStats != null )
      {
        // fully contained
        // the query range covers all the objects in the node
        if ( nodeStats._intersectingRatio >= 1 - Double.MIN_VALUE
            && nodeStats._intersectingRatio <= 1 + Double.MIN_VALUE )
        {
          // have added the result,
          // skip
        }
        // the node have been visited before,
        else if ( nodeStats._intersectingObjectSet.size() > sparseThreshold )
        {
//          // get the result from the join result
//          TreeSet< Integer > result = prefixResultFromNodeLevelJoin.get( nodeHilbertCode );
//          if ( result != null )
//          {
//            // retain the intersecting objects
//            result.retainAll( nodeStatsOfRelaxedRegion._intersectingObjectSet );
//            prefixResultAll.addAll( result );
//          }
          
          nodeStatsOfRelaxedRegion._intersectingObjectSet.retainAll( nodeStats._nonIntersectingObjectSet );

          // if there are few objects needed to be checked
          if ( nodeStatsOfRelaxedRegion._intersectingObjectSet.size() <= sparseThreshold )
          {
            // directly retrieve
            for ( int objectid : nodeStatsOfRelaxedRegion._intersectingObjectSet )
            {
              SpatialObject object = objectDatabase.getSpatialObject( objectid );
              if ( StringVerification.isPrefix( query._queryText, object.getText() ) )
              {
                prefixResultAll.add( objectid );
              }
            }
          }
          else
          {
            // read the second level inverted index and join the result
            this.secondLevelPrefixJoinOutQueryRegion( query, nodeHilbertCode,
                nodeStatsOfRelaxedRegion, prefixResultAll, memObjectMap, true );
          }
          
        }
        // need to check the non intersecting objects
        else
        {
          nodeStatsOfRelaxedRegion._intersectingObjectSet.retainAll( nodeStats._nonIntersectingObjectSet );

          // if there are few objects needed to be checked
          if ( nodeStatsOfRelaxedRegion._intersectingObjectSet.size() <= sparseThreshold )
          {
            // directly retrieve
            for ( int objectid : nodeStatsOfRelaxedRegion._intersectingObjectSet )
            {
              SpatialObject object = objectDatabase.getSpatialObject( objectid );
              if ( StringVerification.isPrefix( query._queryText, object.getText() ) )
              {
                prefixResultAll.add( objectid );
              }
            }
          }
          else
          {
            // read the second level inverted index and join the result
            this.secondLevelPrefixJoinOutQueryRegion( query, nodeHilbertCode,
                nodeStatsOfRelaxedRegion, prefixResultAll, memObjectMap, true );
          }
        }
      }
      // for the non-intersecting nodes
      else
      {
        // get the node stats for the relaxed region.
        this.prefixSingleNodeProcessOfRelaxedRegion( query, nodeHilbertCode,
            nodeStatsOfRelaxedRegion, prefixResultAll, memObjectMap );
      }
    }
  }
  
  
  /**
   * 
   * @param query
   * @param nodeHilbertCode
   * @param nodeStats, if nodeStats is null, the entire node is taken for consideration 
   * @param prefixResultAll
   * @param memObjectMap
   * @param hasIntersection
   */
  private void secondLevelPrefixJoinOutQueryRegion( 
      SpatialQuery query,
      String nodeHilbertCode,
      NodeStatistic nodeStatsOfRelaxedRegion,
      HashSet< Integer > prefixResultAll,
      HashMap< Integer, SpatialObject > memObjectMap,
      boolean hasIntersection )
  {
    // get the object list for each gram in the second level inverted index
    ArrayList< NavigableSet< Integer >> invertedObjectJoinVector = new ArrayList< NavigableSet< Integer >>();

    for ( PositionalQgram positionalQgram : query._firstMPositionalQgramSet )
    {
      if ( (positionalQgram._pos % query._smallQValue != 0) && 
          (positionalQgram._pos != query._queryText.length() - query._smallQValue) )
      {
        continue;
      }
            
      // do not need to have a copy in memory, because it is outside qury region
      int[] objectIdList = secondLevelInvertedIndex.readObjectList( nodeHilbertCode, positionalQgram );

      if ( objectIdList != null )
      {
        TreeSet< Integer > objectList = convertIntArrayToTreeSet( objectIdList );
        invertedObjectJoinVector.add( objectList );
      }
      // the node does not have the positional q-gram, visit next leaf node.
      else
      {
        // no result, return
        invertedObjectJoinVector.clear();
        return;
      }
    }
        
    
    // the join result of the node
    NavigableSet< Integer > nodeResult = SortMergeJoin.join( invertedObjectJoinVector );
    
    if ( nodeResult == null )
    {
      return;
    }
    else
    {     
      // if the nodeStats is specified, need to join with the objects in node
      if ( nodeStatsOfRelaxedRegion != null )
      {
        nodeResult.retainAll( nodeStatsOfRelaxedRegion._intersectingObjectSet );
      }
            
      // if the size is large, need to join again
      if ( nodeResult.size() > sparseThreshold )
      {
        if ( query._largeQgramTokenSetFromM != null )
        {
          invertedObjectJoinVector.clear();
          invertedObjectJoinVector.add( nodeResult );
                    
          for ( String token : query._largeQgramTokenSetFromM )
          {
            String key = nodeHilbertCode + "," + token;
            int[] objectIdList = hilbertQgramTokenInvertedIndex.read( key );

            if ( objectIdList != null )
            {
              TreeSet< Integer > objectList = convertIntArrayToTreeSet( objectIdList );
              invertedObjectJoinVector.add( objectList );
            }
            // the node does not have the positional q-gram, visit next leaf node.
            else
            {
              // no result, return
              invertedObjectJoinVector.clear();
              return;
            }
          }
          
          // join with the inverted list of the q-gram token
          nodeResult = SortMergeJoin.join( invertedObjectJoinVector );
          if ( nodeResult == null )
          {                       
            return;
          }           
        }
      }
    }
     
      
    // if the query text is not too long, need to verify the prefix condition
    if ( query._largeQgramTokenSetFromM != null )
    {
      Iterator< Integer > itr = nodeResult.iterator();
      
      if ( hasIntersection )
      {
        while ( itr.hasNext() )
        {
          int objectid = itr.next();          
          // if the previous result does not contain the object id
          if ( ! prefixResultAll.contains( objectid ) )
          {
            // read the object
            SpatialObject object = memObjectMap.get( objectid );
            if ( object == null )
            {
              object = objectDatabase.getSpatialObject( objectid );
            }               
            
            // if it is not a prefix, remove it
            if ( ! StringVerification.isPrefix( query._queryText, object._text ) )
            {
              itr.remove();
            }
          }            
        }
      }
      else
      {
        while ( itr.hasNext() )
        {
          int objectid = itr.next();
          SpatialObject object = objectDatabase.getSpatialObject( objectid );
      
          // if it is not a prefix, remove it
          if ( ! StringVerification.isPrefix( query._queryText, object._text ) )
          {
            itr.remove();
          }
        }
      }          
    }           
    
    // save and return the result     
    prefixResultAll.addAll( nodeResult );    
  }
  
  
  
  public HashMap< Integer, SimpleEntry < QueryType, SpatialObject >> substringQueryOnly( SpatialQuery query )
  {   
    // results
    HashMap< Integer, SimpleEntry < QueryType, SpatialObject >> resultMap = new HashMap< Integer, SimpleEntry < QueryType, SpatialObject >>();                 
        
    query._queryType = QueryType.SUBSTRING_RANGE;
    
    // check whether there is infrequent qgram
    BooleanObject hasInfrequentQgramToken = new BooleanObject( false );     
    HashMap< String, TreeSet< Integer >> memInfreqQgramTokenInvertedMap = new HashMap< String, TreeSet< Integer >>();
                                        
    this.getInfrequentQgramTokenMapping( memInfreqQgramTokenInvertedMap, query._largeQgramTokenSet, hasInfrequentQgramToken );        
        
    // has infrequent q-gram token
    if ( hasInfrequentQgramToken.isTrue() )
    {
      NavigableSet< Integer > joinedResult = infrequentQgramExactJoin( memInfreqQgramTokenInvertedMap.values() );
      if ( joinedResult != null )
      {        
        resultMap = getSubstringInfrequentResult( query, joinedResult );       
      }            
    }
    // check if the query text does not contain any infrequent q-gram token
    else
    {
      // get the intersecting node statistics          
      IntersectingNodeStatsMap intersectingNodeStatistic = new IntersectingNodeStatsMap();
      quadtree.getIntersectingNodeStatistic( query._queryRegion, intersectingNodeStatistic, sparseThreshold );
                             
      substringRangeQuery( query, intersectingNodeStatistic, resultMap );    
     }
    return resultMap;    
  }

  
  protected void substringNodeFilterInPrefix( SpatialQuery query,  
      BooleanObject doSubstringNodeFilterInPrefixQuery,
      HashMap< String, HilbertCountMap > memQgramTokenCountPairInvertedMap,
      IntersectingNodeStatsMap intersectingNodeStatsMapOfRelaxedRegion,
      TreeMap< String, ArrayList< Integer >> substringNodeCandidateCountMap )
  {
    // only run once, read the large q-gram from M
    if ( doSubstringNodeFilterInPrefixQuery.isTrue() )
    {
      return;
    }      
    doSubstringNodeFilterInPrefixQuery.setTrue();
    
    ArrayList< HilbertCountMap > joinList = new ArrayList< HilbertCountMap >();    
    IntersectionJoin<Integer, NodeStatistic> intersectionJoin = new IntersectionJoin<Integer, NodeStatistic>(); 
    boolean isFirst = true;
    
    
    if ( query._largeQgramTokenSetFromM != null )
    {
      for ( String token : query._largeQgramTokenSetFromM )
      {
        HilbertCountMap map = qgramTokenCountPairInvertedIndex.read( token );
        if ( map != null )
        {   
          if ( !map.isEmpty() )
          {
            // save in memory
            if( isFirst )
            {
              intersectionJoin.join( map._map, intersectingNodeStatsMapOfRelaxedRegion );
              isFirst = false;
            }
            
            if ( map.isEmpty() )
            {
              memQgramTokenCountPairInvertedMap.put( token, new HilbertCountMap() );
              joinList.clear();
              return;
            }
            else
            {
              memQgramTokenCountPairInvertedMap.put( token, map );                      
              joinList.add( map );
            }
          }
          else
          {
            memQgramTokenCountPairInvertedMap.put( token, new HilbertCountMap() );
            joinList.clear();
            return;
          }
        }
        else
        {
          joinList.clear();
          return;
        }
      }
    }
    
    if ( ! joinList.isEmpty() )
    {
      NodeLevelJoin joinOperator = new NodeLevelJoin();
      TreeMap< String, ArrayList< Integer >> result = joinOperator.join( joinList );

      // copy to the substring node candidate map
      if ( result != null )
      {
        Iterator< Entry< String, ArrayList< Integer >>> itr = result.entrySet().iterator();
        while ( itr.hasNext() )
        {
          Entry< String, ArrayList< Integer >> entry = itr.next();
          substringNodeCandidateCountMap.put( entry.getKey(), entry.getValue() );
        }
      }
    }    
  }
  
  
  
  protected void substringNodeFilterInPrefix( 
      SpatialQuery query,           
      IntersectingNodeStatsMap intersectingNodeStats,
      TreeMap< String, ArrayList< Integer >> substringNodeCandidateCountMap )
  {
    ArrayList< HilbertCountMap > joinList = new ArrayList< HilbertCountMap >();
    IntersectionJoin< Integer, NodeStatistic > intersectionJoin = new IntersectionJoin< Integer, NodeStatistic >();
    boolean isFirst = true;

    if ( query._largeQgramTokenSetFromM != null )
    {
      for ( String token : query._largeQgramTokenSetFromM )
      {
        HilbertCountMap map = qgramTokenCountPairInvertedIndex.read( token );
        if ( map != null )
        {
          if ( !map.isEmpty() )
          {            
            if ( isFirst )
            {
              intersectionJoin.join( map._map, intersectingNodeStats );
              isFirst = false;
            }

            if ( map.isEmpty() )
            {              
              joinList.clear();
              return;
            }
            else
            {             
              joinList.add( map );
            }
          }
          else
          {           
            joinList.clear();
            return;
          }
        }
        else
        {
          joinList.clear();
          return;
        }
      }
    }

    if ( !joinList.isEmpty() )
    {
      NodeLevelJoin joinOperator = new NodeLevelJoin();
      TreeMap< String, ArrayList< Integer >> result = joinOperator.join( joinList );

      // copy to the substring node candidate map
      if ( result != null )
      {
        Iterator< Entry< String, ArrayList< Integer >>> itr = result.entrySet().iterator();
        while ( itr.hasNext() )
        {
          Entry< String, ArrayList< Integer >> entry = itr.next();
          substringNodeCandidateCountMap.put( entry.getKey(), entry.getValue() );
        }
      }
    }
  }
  
  
  
  protected void substringNodeFilter( SpatialQuery query,
      BooleanObject doSubstringNodeFilterInPrefixQuery,
      BooleanObject doSubstringNodeFilter,
      HashMap< String, HilbertCountMap > memQgramTokenCountPairInvertedMap,
      IntersectingNodeStatsMap intersectingNodeStatsMapOfRelaxedRegion,
      TreeMap< String, ArrayList< Integer >> substringNodeCandidateCountMap )
  {
    // only run once
    if ( doSubstringNodeFilter.isTrue() )      
    {
      return;
    } 
    doSubstringNodeFilter.setTrue();    
    
    ArrayList< HilbertCountMap > joinList = new ArrayList< HilbertCountMap >();
    IntersectionJoin<Integer, NodeStatistic> intersectionJoin = new IntersectionJoin<Integer, NodeStatistic>(); 
    boolean isFirst = true;
    
    if ( query._largeQgramTokenSet != null )
    {      
      for ( String token : query._largeQgramTokenSet )
      {
        if ( doSubstringNodeFilterInPrefixQuery.isTrue() )
        {
          if ( query._largeQgramTokenSetFromM != null )
          {
            if ( query._largeQgramTokenSetFromM.contains( token ) )
            {
              continue;
            }
          }
        }
        
        HilbertCountMap map = qgramTokenCountPairInvertedIndex.read( token );
        if ( map != null )
        {
          if ( !map.isEmpty() )
          {
            // save in memory            
            if ( isFirst )
            {
              intersectionJoin.join( map._map, intersectingNodeStatsMapOfRelaxedRegion );
              isFirst = false;
            }
            
            if ( map.isEmpty() )
            {
              memQgramTokenCountPairInvertedMap.put( token, new HilbertCountMap() );                        
              joinList.clear();
              substringNodeCandidateCountMap.clear();
              return;
            }
            else
            {
              memQgramTokenCountPairInvertedMap.put( token, map );                        
              joinList.add( map );
            }
          }
          else
          {
            memQgramTokenCountPairInvertedMap.put( token, new HilbertCountMap() );
            joinList.clear();
            substringNodeCandidateCountMap.clear();
            return;
          }
        }
        else
        {
          joinList.clear();
          substringNodeCandidateCountMap.clear();
          return;
        }
      }           
    }

    if ( ! joinList.isEmpty() )
    {
      NodeLevelJoin joinOperator = new NodeLevelJoin();
      TreeMap< String, ArrayList< Integer >> result = joinOperator.join( joinList );

      // copy to the substring node candidate map
      if ( result != null )
      {        
        // join the result
        // if do the substring node filter before
        if ( doSubstringNodeFilterInPrefixQuery.isTrue() )
        {
          // join the substringCandidate nodes with the results
          IntersectionJoin< ArrayList<Integer>, ArrayList< Integer > > IntersectionJoinOperation = new  
              IntersectionJoin< ArrayList<Integer>, ArrayList< Integer > >();
          
          IntersectionJoinOperation.join( substringNodeCandidateCountMap, result );
                    
          if ( substringNodeCandidateCountMap.isEmpty() )
          {
            return;
          }
        }    
               
        Iterator< Entry< String, ArrayList< Integer >>> itr = result.entrySet().iterator();
        while ( itr.hasNext() )
        {
          Entry< String, ArrayList< Integer >> entry = itr.next();          
          String nodeHilbertCode = entry.getKey();
          ArrayList< Integer > countList = entry.getValue();
                    
          // if never do the substring node filter
          if ( doSubstringNodeFilterInPrefixQuery.isFalse() )
          {
            substringNodeCandidateCountMap.put( nodeHilbertCode, countList );
          }
          else
          {           
            ArrayList< Integer > storedList = substringNodeCandidateCountMap.get( nodeHilbertCode );
            if ( storedList != null )
            {
              storedList.addAll( countList );
            }          
          }
          itr.remove();
        }
      }
      else 
      {
        substringNodeCandidateCountMap.clear();
      }
    }
  }
  
  

  private void substringIntersectingNodeFilter( 
      TreeMap< String, ArrayList< Integer >> substringNodeCandidateMap,
      IntersectingNodeStatsMap intersectingNodeStatistic
      )
  {
    IntersectionJoin< ArrayList<Integer>, NodeStatistic > intersectionJoinOperation = 
        new IntersectionJoin< ArrayList<Integer>, NodeStatistic >();
    intersectionJoinOperation.join( substringNodeCandidateMap, intersectingNodeStatistic );
  }
  
  
  private void readSmallQgramTokenHilbertCountPair( 
      SpatialQuery query,
      BooleanObject hasReadSmallQgram,
      HashMap< String, HilbertCountMap > memQgramTokenCountPairInvertedMap,
      TreeMap< String, ArrayList< Integer >> substringNodeCandidateCountMap )
  {
    // only run once, if has been run before, exit
    if ( hasReadSmallQgram.isTrue() )
    {
      return;
    }
     
    hasReadSmallQgram.setTrue();
    IntersectionJoin< Integer, ArrayList<Integer> > joinOperator = new IntersectionJoin< Integer, ArrayList<Integer> >();
  
    for ( String token : query._qgramSetWithoutWildCard )
    {           
      HilbertCountMap hilbertCountMap = qgramTokenCountPairInvertedIndex.read( token );
      if ( hilbertCountMap != null )
      {                
        joinOperator.join( hilbertCountMap._map, substringNodeCandidateCountMap );
        memQgramTokenCountPairInvertedMap.put( token, hilbertCountMap );
      } 
    }
  }
  
  


  

  public void substringRangeQuery( 
      SpatialQuery query,       
      IntersectingNodeStatsMap intersectingNodeStatsMap,
      HashMap< Integer, SimpleEntry < QueryType, SpatialObject >> resultMap)
  {               
    // if the number of intersecting node is small, do not need to do node filter.
    if ( intersectingNodeStatsMap.denseNodeNumber < visitingNodeSizeThreshold )
    {          
      Iterator< Entry< String, NodeStatistic >> itr = intersectingNodeStatsMap.entrySet().iterator();
      while ( itr.hasNext() )
      {
        Entry< String, NodeStatistic > entry = itr.next();
        String nodeHilbertCode = entry.getKey();
        NodeStatistic nodeStats = entry.getValue();

        // substring query in node
        substringQueryInSingleNode( query, nodeHilbertCode, nodeStats, resultMap);                
      }     
    }
    
    // if the number of intersecting node is large, need to do node filter.
    else
    {    
      // compute the substringNodeCandidateMap      
      TreeMap< String, ArrayList< Integer >> substringNodeCandidateMap = new TreeMap< String, ArrayList< Integer >> ();
      this.substringNodeFilter( query,  new BooleanObject(false), new BooleanObject(false),
        new HashMap<String, HilbertCountMap>(), intersectingNodeStatsMap, substringNodeCandidateMap );
         
      substringIntersectingNodeFilter( substringNodeCandidateMap, intersectingNodeStatsMap );
            
      Iterator< Entry< String, ArrayList< Integer >>> nodeItr = substringNodeCandidateMap.entrySet().iterator();
      
      while ( nodeItr.hasNext() )
      {
        Entry< String, ArrayList< Integer >> entry = nodeItr.next();
        String nodeHilbertCode = entry.getKey();
        NodeStatistic nodeStats = intersectingNodeStatsMap.get( nodeHilbertCode );

        substringQueryInSingleNode( query, nodeHilbertCode, nodeStats, resultMap);                
      }
    }   
  }
  
  
  
  
  protected void substringQueryInSingleNode ( 
      SpatialQuery query,       
      String nodeHilbertCode,
      NodeStatistic nodeStats,
      HashSet< Integer > substringResult,
      HashMap< Integer, SpatialObject > memObjectMap,
      HashMap< String, TreeSet< Integer >> memHilbQgramTokenInvertedMap )
  {
    if ( nodeStats._intersectingObjectSet.size() <= sparseThreshold )
    {
      for ( int objectid : nodeStats._intersectingObjectSet )
      {
        if ( ! substringResult.contains( objectid ) )
        {
          SpatialObject object = memObjectMap.get( objectid );
          if ( object == null )
          {
            object = objectDatabase.getSpatialObject( objectid );
            memObjectMap.put( objectid, object );
          }
          
          if ( query.isSubstring( object._text ) )
          {
            substringResult.add( objectid );
          }
        }                              
      }
    }
    else
    {
      boolean containsNode = 
          ( ( nodeStats._intersectingRatio <= 1 - Double.MIN_VALUE )  && ( nodeStats._intersectingRatio >= 1 + Double.MIN_VALUE ) ) ? true : false;
      
      secondLevelSubstringJoin( query, substringResult, nodeHilbertCode, nodeStats, containsNode, memObjectMap, memHilbQgramTokenInvertedMap );
    }
  }
  
  
  
  protected void substringQueryInSingleNode ( 
      SpatialQuery query,       
      String nodeHilbertCode,
      NodeStatistic nodeStats,
      HashMap< Integer, SimpleEntry < QueryType, SpatialObject >> resultMap)
  {
    // directly retrieve the objects
    if ( nodeStats._intersectingObjectSet.size() <= sparseThreshold )
    {
      for ( int objectid : nodeStats._intersectingObjectSet )
      {               
        SpatialObject object = objectDatabase.getSpatialObject( objectid );
                   
        if ( query.isSubstring( object._text ) )
        {
          resultMap.put( objectid,  new SimpleEntry<QueryType, SpatialObject> (QueryType.SUBSTRING_RANGE, object) );
        }                                    
      }
    }
    // perform a join operation
    else
    {       
      secondLevelSubstringJoin( query, nodeHilbertCode, nodeStats, resultMap );
    }
  }
  
    
  
  
  private void secondLevelSubstringJoin( 
        SpatialQuery query,
        HashSet< Integer > substringResult,
        String nodeHilbertCode,
        NodeStatistic nodeStats,
        boolean containsNode,
        HashMap< Integer, SpatialObject > memObjectMap,
        HashMap< String, TreeSet< Integer >> memHilbQgramTokenInvertedMap )
  {
    
    ArrayList< NavigableSet< Integer >> objectJoinList = new ArrayList< NavigableSet< Integer >>();     
             
    for ( String token : query._largeQgramTokenSet )
    {
      String key = nodeHilbertCode + "," + token;
      // get the object list
      TreeSet< Integer > objectList = memHilbQgramTokenInvertedMap.get( key );
      
      // in memory
      if ( objectList != null )
      {
        if ( ! objectList.isEmpty() )
        {
          objectJoinList.add( objectList );
        }
        else
        {
          // no result
          objectJoinList.clear();
          return;
        }
      }
      
      // retrieve in disk
      else
      {
        // read it from the disk
        int[] objects = hilbertQgramTokenInvertedIndex.read( key );
        if ( objects != null )
        {
          objectList = convertIntArrayToTreeSet( objects );
          memHilbQgramTokenInvertedMap.put( key, objectList );
          objectJoinList.add( objectList );
        }
        else
        {
          // no result
          objectJoinList.clear();
          memHilbQgramTokenInvertedMap.put( key, new TreeSet< Integer >() );
          return;
        }
      }
    }
     
        
    // if the program can reach here, join the object list
    NavigableSet< Integer > substringCandidates = SortMergeJoin.join( objectJoinList );
    if ( substringCandidates != null )
    {
      // remove all the results that the subtringResult already contains
      substringCandidates.removeAll( substringResult );
      
      // if the node is not contained by the query region, get the intersecting objects
      if ( ! containsNode )
      {
        substringCandidates.retainAll( nodeStats._intersectingObjectSet );        
      }
      
      // check the textual condition
      for ( int objectid : substringCandidates )
      {
        SpatialObject object = memObjectMap.get( objectid );
        if ( object == null )
        {
          object = objectDatabase.getSpatialObject( objectid );
          memObjectMap.put( objectid, object );
        }
         
        if ( query.isSubstring( object._text ) )
        {
          substringResult.add( objectid );
        }
      }           
    }        
  }
  
  
  
  private void secondLevelSubstringJoin( 
      SpatialQuery query,       
      String nodeHilbertCode, 
      NodeStatistic nodeStats,       
      HashMap< Integer, SimpleEntry < QueryType, SpatialObject >> resultMap )
  {
    ArrayList< NavigableSet< Integer >> objectJoinList = new ArrayList< NavigableSet< Integer >>();

    for ( String token : query._largeQgramTokenSet )
    {
      String key = nodeHilbertCode + "," + token;
      // get the object list
      TreeSet< Integer > objectList;
     
      // read it from the disk
      int[] objects = hilbertQgramTokenInvertedIndex.read( key );
      if ( objects != null )
      {
        objectList = convertIntArrayToTreeSet( objects );        
        objectJoinList.add( objectList );
      }
      else
      {
        // no result
        objectJoinList.clear();       
        return;
      }
    }
    
    // if the program can reach here, join the object list
    NavigableSet< Integer > substringCandidates = SortMergeJoin.join( objectJoinList );
    if ( substringCandidates != null )
    {      
      // if the node is not contained by the query region, get the intersecting objects      
      if ( ! ( ( nodeStats._intersectingRatio <= 1 - Double.MIN_VALUE )  && ( nodeStats._intersectingRatio >= 1 + Double.MIN_VALUE ) ) )
      {
        substringCandidates.retainAll( nodeStats._intersectingObjectSet );
      }

      // check the textual condition
      for ( int objectid : substringCandidates )
      {
        SpatialObject object = objectDatabase.getSpatialObject( objectid );
        
        if ( query.isSubstring( object._text ) )
        {
          resultMap.put( objectid,  new SimpleEntry<QueryType, SpatialObject> (QueryType.SUBSTRING_RANGE, object) );
        }
      }
    }
  }

  
  
  public boolean isSubstringSelectivityExeedSelectivityThreshold( 
      SpatialQuery query,
      BooleanObject doSubstringNodeFilterInPrefixQuery,
      BooleanObject doSubstringNodeFiler,
      BooleanObject hasReadSmallQgram,
      TreeMap< String, ArrayList< Integer >> substringNodeCandidateCountMap,
      IntersectingNodeStatsMap intersectingNodeStatsMap,
      HashMap< String, TreeSet< Integer >> prefixResultMap,
      HashMap< String, HilbertCountMap > memQgramTokenCountPairInvertedMap,
      HashMap< String, TreeSet<Integer> > memHilbQgramTokenInvertedMap,
      HashMap< String, Double > substringSelectivityMap )
  {
    double estimatedSize = 0.0;    
    
    // when the size of the intersecting nodes is large, need to do substring node filter
    if ( intersectingNodeStatsMap.denseNodeNumber >= visitingNodeSizeThreshold )
    {      
      // do the substring node filter
      if ( query._largeQgramTokenSet != null )
      {     
         this.substringNodeFilter( query, doSubstringNodeFilterInPrefixQuery, doSubstringNodeFiler,
           memQgramTokenCountPairInvertedMap, intersectingNodeStatsMap, substringNodeCandidateCountMap );      
      }              
      if ( substringNodeCandidateCountMap == null || substringNodeCandidateCountMap.isEmpty() )
      {
        return ( 0 >= selectivityThreshold );
      }
    }
    // else the number of the intersecting nodes is small, do not need to do substring node filter
    
    
    Iterator< Entry< String, NodeStatistic >> nodeItr = intersectingNodeStatsMap.entrySet().iterator();           
    // get the substring selectivity for each intersecting node
    while( nodeItr.hasNext() )
    {
      Entry< String, NodeStatistic > entry = nodeItr.next();
      String nodeHilbertCode = entry.getKey();            
      NodeStatistic nodeStats = entry.getValue();
      
      // check whether the substring node candidate map contains the node hilbert code
      ArrayList< Integer > numeratorList = null;      
      double nodeSubstringEstimatedSize = 0.0;
      int numberOfObjectInNode = nodeStats._numberOfObjects;
      
      // if have do the substring node filter before, get the selectivity value
      if ( doSubstringNodeFiler.isTrue() )
      {
        numeratorList = substringNodeCandidateCountMap.get( nodeHilbertCode );
        if ( ( numeratorList != null ) && ( ! numeratorList.isEmpty() ) )
        {
          nodeSubstringEstimatedSize =  getSubstringSelectivityOfSingleNode( query, 
            hasReadSmallQgram, nodeHilbertCode, scarceThreshold, numberOfObjectInNode, numeratorList,
            memQgramTokenCountPairInvertedMap, substringNodeCandidateCountMap, substringSelectivityMap );  
        }
        else
        {
          continue;
        }               
      }
      
      // else get the maximum selectivity value
      else
      {
        nodeSubstringEstimatedSize =  this.getSubstringMaxSelectivityOfSingleNodeInObjectLevel( 
            query, nodeHilbertCode, memHilbQgramTokenInvertedMap, substringSelectivityMap );
      }
      
      double intersectingRatio = nodeStats._intersectingRatio;
                 
      nodeSubstringEstimatedSize *= intersectingRatio;
      
      int nodePrefixResultSize = 0;
      NavigableSet< Integer > nodePrefixResult = prefixResultMap.get( nodeHilbertCode );
      if ( nodePrefixResult != null )
      {
        nodePrefixResultSize = nodePrefixResult.size();
      }
      
      estimatedSize += Math.max( nodeSubstringEstimatedSize, nodePrefixResultSize );
      
      if ( estimatedSize >= selectivityThreshold )
      {
        return true;
      }
    }       
    return ( estimatedSize >= selectivityThreshold );
  }

  
  private double getSubstringSelectivityOfSingleNode( 
      SpatialQuery query, 
      BooleanObject hasReadSmallQgram,
      final String hilbertCode,
      final double scarceThreshold,
      final int objectNumberInNode,
      final ArrayList< Integer > numeratorList,      
      final HashMap< String, HilbertCountMap > memQgramTokenCountPairInvertedMap,
      final TreeMap< String, ArrayList< Integer > > substringNodeCandidateCountMap,
      final HashMap< String, Double > substringSelectivityMap )
  {    
   
    // computed before in the prefix selectivity
    Double storedSelectivity = substringSelectivityMap.get( hilbertCode );
    if ( storedSelectivity != null )
    {
      return storedSelectivity;
    }
            
    // do not compute in the prefix selectivity
    if ( query._largeQgramTokenSet != null )
    { 
      double selectivity = 1.0;         

      ArrayList< Integer > normalCountList = substringNodeCandidateCountMap.get( hilbertCode );
      if ( normalCountList == null )
      {
        System.err.println( "Error in SpatialInstaantQuery.getPrefixSelectivityOfSingleNode() " );
        return -1;
      }
      int normalCountMin = getMinimumValue( normalCountList );
      
      
      // if there is a scarce q-gram
      if ( ( normalCountMin / (objectNumberInNode * 1.0) ) < scarceThreshold )
      {
        return normalCountMin;
      }
      
      
      for ( int numerator : normalCountList )
      {
        selectivity *= ( numerator * 1.0 );
      }

      
      // read the small q-gram token
      this.readSmallQgramTokenHilbertCountPair( query, hasReadSmallQgram, memQgramTokenCountPairInvertedMap, substringNodeCandidateCountMap );
      for ( String smallToken : query._qgramSetWithoutWildCard )
      {
        HilbertCountMap map = memQgramTokenCountPairInvertedMap.get( smallToken );
        int denominator = map._map.get( hilbertCode );
        selectivity /= ( denominator * 1.0 );
      }

      HilbertCountMap map = memQgramTokenCountPairInvertedMap.get( query._queryText.substring( 0, query._smallQValue ) );
      Integer firstSmallQgramCount = map._map.get( hilbertCode );

      double substringSelectivity = selectivity * firstSmallQgramCount;
     
      return Math.min( substringSelectivity, normalCountMin ) ;
    }
    
    // the query length is short
    else
    {
      String firstSmallToken = query._queryText.substring( 0, query._smallQValue ); 
      HilbertCountMap map = memQgramTokenCountPairInvertedMap.get( firstSmallToken );
      if ( map == null )
      {
        map = qgramTokenCountPairInvertedIndex.read( firstSmallToken );
      }
      return map._map.get( hilbertCode ) ;          
    }   
  }
  
  private double getSubstringMaxSelectivityOfSingleNodeInObjectLevel( 
      SpatialQuery query,
      final String hilbertCode,
      final HashMap< String, TreeSet<Integer> > memHilbQgramTokenInvertedMap,
      final HashMap< String, Double > substringSelectivityMap )
  {
    // the selectivity value is computed before, return the value
    Double maxSubstringSelectivity = substringSelectivityMap.get( hilbertCode );
    if ( maxSubstringSelectivity != null )
    {
      return maxSubstringSelectivity;
    }
        
    int minCount = Integer.MAX_VALUE;    
    if ( query._largeQgramTokenPositionMap == null )
    {
      System.err.println( "error in SpatialInstantQuery:getSubstringMaxSelectivityOfSingleNodeInObjectLevel()" );
      return -Integer.MAX_VALUE;
    }
    else      
    {
      for( String token : query._largeQgramTokenPositionMap.keySet() )
      {
        String key = hilbertCode + "," + token;
        TreeSet<Integer> idSet = memHilbQgramTokenInvertedMap.get( key );
        
        // read from disk
        if ( idSet == null )
        {
          idSet = hilbertQgramTokenInvertedIndex.getIdSet( key );
          if ( idSet == null )
          {
            memHilbQgramTokenInvertedMap.put( key, new TreeSet<Integer>() );
            return 0;
          }
          else
          {
            memHilbQgramTokenInvertedMap.put( key, idSet );
            minCount = Math.min( minCount, idSet.size() );
          }
        }
        // already in memory
        else
        {
          if ( idSet.isEmpty() )
          {
            return 0;
          }
          else
          {
            minCount = Math.min( minCount, idSet.size() );
          }
        }
      }
    }    
    return minCount;
  }
  
  
  public HashMap< Integer, SimpleEntry < QueryType, SpatialObject >> approPrefixQueryOnly( SpatialQuery query )
  {    
    HashMap< Integer, SimpleEntry < QueryType, SpatialObject > > results = new HashMap< Integer, SimpleEntry < QueryType, SpatialObject > >();
        
    
    // apply approximate prefix when the query length is greater than 3    
    if ( query._queryText.length() > 3 )
    {
      query._queryType = QueryType.APPROXIMATE_PREFIX_RANGE;
      
      // get the intersecting node statistics        
      IntersectingNodeStatsMap intersectingNodeStatsMap = new IntersectingNodeStatsMap();
      quadtree.getIntersectingNodeStatistic( query._queryRegion, intersectingNodeStatsMap, sparseThreshold );           
      
      // approximate prefix query in the given spatial range                                        
      QgramFilter qgramFilter = new QgramFilter ( query._largeQgramGenerator );  
      MergeSkip mergeSkipOperator = new MergeSkip ();
            
      this.approximatePrefixRangeQuery( query, results, query._minQgramTokenMatch,
        intersectingNodeStatsMap, qgramFilter, mergeSkipOperator );                                                 
    }
    return results;
  }
    
  
  
  
  public void approximatePrefixRangeQuery( 
      SpatialQuery query,
      HashMap< Integer, SimpleEntry < QueryType, SpatialObject >> resultMap,  
      int tokenMinMatchThreshold,
      IntersectingNodeStatsMap intersectingNodeStatsMap,      
      QgramFilter qgramFilter,
      MergeSkip mergeSkipOperator )
  {
    // do not need to do node filter
    if ( intersectingNodeStatsMap.denseNodeNumber < visitingNodeSizeThreshold )
    {
      Iterator< Entry< String, NodeStatistic >> itr =
          intersectingNodeStatsMap.entrySet().iterator();
      while ( itr.hasNext() )
      {
        Entry< String, NodeStatistic > entry = itr.next();
        String nodeHilbertCode = entry.getKey();
        NodeStatistic nodeStats = entry.getValue();

        this.approximatePrefixQueryInSingleNode( query, resultMap, nodeHilbertCode,
            nodeStats, qgramFilter, mergeSkipOperator );                        
                                  
      }
    }

    // need to do node filter
    else
    {
      TreeSet< String > approximatePrefixNodeCandidates = new TreeSet< String >();
      this.approximatePrefixNodeFilter( query, tokenMinMatchThreshold, intersectingNodeStatsMap, approximatePrefixNodeCandidates) ;
      
      if ( !approximatePrefixNodeCandidates.isEmpty() )
      {
        for ( String nodeHilbertCode : approximatePrefixNodeCandidates )
        {
          NodeStatistic nodeStats = intersectingNodeStatsMap.get( nodeHilbertCode );
          
          this.approximatePrefixQueryInSingleNode( query, resultMap, nodeHilbertCode,
            nodeStats, qgramFilter, mergeSkipOperator );                                 
        }
      }
    }
  }
  
  
  private void browsePrefixInfrequentResult( 
	      SpatialQuery query,
	      NavigableSet< Integer > infrequentJoinedResult, 
	      SpatialObjectDatabase objectDatabase,
	      BooleanObject hasReadInfrequentToken,
	      ArrayList< Integer > prefixResult,
	      HashMap< Integer, SpatialObject > visitedObjectMap,
	      ArrayList< Integer > prefixResultFromInfrequentQgram,
	      ArrayList< Integer > substringCandidates )
	  {
	    for ( int objectid : infrequentJoinedResult )
	    {
	      SpatialObject object = objectDatabase.getSpatialObject( objectid );
	      
	      // save to visited map
	      visitedObjectMap.put( objectid, object );

	      if ( hasReadInfrequentToken.isTrue() )
	      {       
	        substringCandidates.add( objectid );
	      }

	      // check prefix condition
	      if ( StringVerification.isPrefix( query._queryText, object._text ) )
	      {
	        // the prefix result in the whole data set
	        prefixResultFromInfrequentQgram.add( objectid );

	        if ( query._queryRegion.contains( object.getPoint() ) )
	        {
	          // the prefix result in the given spatial range
	          prefixResult.add( objectid );
	        }
	      }
	    }
	  }
  protected void approximatePrefixNodeFilter( 
      SpatialQuery query,
      int tokenMinMatchThreshold,
      IntersectingNodeStatsMap intersectingNodeStatsMap,
      BooleanObject doApproximateSubstringNodeFilter,
      TreeSet<String> approximatePrefixNodeCandidates,
      TreeSet<String> approximateSubstringNodeCandidates,
      HashMap< PositionalQgram, HilbertCountMap > memPosQgramCountPairInvertedMap,
      HashMap< String, HilbertCountMap > memQgramTokenCountPairInvertedMap )
  {
    // if query length is less than 20 characters
    // use prefix filter       
    IntersectionJoin< Integer, NodeStatistic > intersectionJoinOperator = new IntersectionJoin< Integer, NodeStatistic >();
    
    if ( query._queryText.length() <= 20 )
    {
      // new change
      int prefixFilterLength = query._tau * query._smallQValue;
     
      
      // prefix filter, each node must have one matching q-gram
      for ( PositionalQgram posQgram : query._firstMPositionalQgramSet )
      {
        if ( posQgram._pos <= prefixFilterLength )
        {
          // get the matching positional q-grams
          for ( int i = (- query._tau); i <= query._tau; i++ )
          {            
            int matchingPosition = posQgram._pos + i;
            if ( matchingPosition >= 0 && matchingPosition <= prefixFilterLength )
            {
              // get the matching q-gram
              PositionalQgram matchingPosQgram =  new PositionalQgram( posQgram._qgram, matchingPosition );
              HilbertCountMap hilbertCountMap = memPosQgramCountPairInvertedMap.get( matchingPosQgram );

              if ( hilbertCountMap != null )
              {
                intersectionJoinOperator.join( hilbertCountMap._map, intersectingNodeStatsMap );
                approximatePrefixNodeCandidates.addAll( hilbertCountMap._map.keySet() );
              }
              else
              {
                hilbertCountMap = firstLevelInvertedIndex.read( matchingPosQgram );
                
                if ( hilbertCountMap != null )
                {
                  intersectionJoinOperator.join( hilbertCountMap._map, intersectingNodeStatsMap );
                  memPosQgramCountPairInvertedMap.put( matchingPosQgram, hilbertCountMap );
                  approximatePrefixNodeCandidates.addAll( hilbertCountMap._map.keySet() );
                }
                else
                {
                  memPosQgramCountPairInvertedMap.put( matchingPosQgram, new HilbertCountMap() );
                }
              }
            }
          }
        }
      }
      
      // if the number of candidate nodes is small, do not need to applied other filters
      if ( approximatePrefixNodeCandidates.size() < visitingNodeSizeThreshold )
      {
        return;
      }      
    
      // else apply other filtering methods for the nodes
      if ( query._minPosQgramMatch >= 1 )
      {             
        // if the text is short, apply the count filter for positional q-gram
        if ( query._queryText.length() - query._smallQValue + 1  <= lamdaValue - query._tau )
        {
          TreeSet< String > approximatePrefixNodeCandidatesForShortText = new TreeSet<String>();      
          
          approximatePrefixNodeFilterForShortText( query, intersectingNodeStatsMap, query._minPosQgramMatch,
            approximatePrefixNodeCandidatesForShortText, memPosQgramCountPairInvertedMap );                
       
          // join the result
          if ( approximatePrefixNodeCandidatesForShortText.isEmpty() )
          {
            approximatePrefixNodeCandidates.clear();
            return;
          }
          else
          {
            intersectionJoinOperator.join( approximatePrefixNodeCandidates, approximatePrefixNodeCandidatesForShortText );
          }
        }
        
        // if the size id too large, apply the count filter
        if ( approximatePrefixNodeCandidates.size() >= visitingNodeSizeThreshold )
        {
          if ( tokenMinMatchThreshold >= 1 )
          {                        
            // substring node filter
            this.approximateSubstringNodeFilter( query, tokenMinMatchThreshold,
                intersectingNodeStatsMap, doApproximateSubstringNodeFilter,
                approximateSubstringNodeCandidates, memQgramTokenCountPairInvertedMap );
  
            // no node having the q-gram count greater than the threshold
            if ( approximateSubstringNodeCandidates.isEmpty() )
            {
              approximatePrefixNodeCandidates.clear();
            }
            else
            {
              intersectionJoinOperator.join( approximatePrefixNodeCandidates, approximateSubstringNodeCandidates );
            }          
          }  
        }
              
      }        
    }
    
    // the query text is long
    else
    {           
      // use the approximate substring node filter
      this.approximateSubstringNodeFilter( query, tokenMinMatchThreshold,
          intersectingNodeStatsMap, doApproximateSubstringNodeFilter,
          approximateSubstringNodeCandidates, memQgramTokenCountPairInvertedMap );

      // no node having the q-gram count greater than the threshold
      if ( ! approximateSubstringNodeCandidates.isEmpty() )
      {
        approximatePrefixNodeCandidates.addAll( approximateSubstringNodeCandidates );
      }            
    }    
  }


  protected void approximatePrefixNodeFilter( 
      SpatialQuery query,
      int tokenMinMatchThreshold,
      IntersectingNodeStatsMap intersectingNodeStatsMap,     
      TreeSet<String> approximatePrefixNodeCandidates )
  {
    // if query length is less than 20 characters
    // use prefix filter       
    IntersectionJoin< Integer, NodeStatistic > intersectionJoinOperator = new IntersectionJoin< Integer, NodeStatistic >();
    
    if ( query._queryText.length() <= 20 )
    {
      // new change
      int prefixFilterLength = query._tau * query._smallQValue;
     
      
      // prefix filter, each node must have one matching q-gram
      for ( PositionalQgram posQgram : query._firstMPositionalQgramSet )
      {
        if ( posQgram._pos <= prefixFilterLength )
        {
          // get the matching positional q-grams
          for ( int i = (- query._tau); i <= query._tau; i++ )
          {            
            int matchingPosition = posQgram._pos + i;
            if ( matchingPosition >= 0 && matchingPosition <= prefixFilterLength )
            {
              // get the matching q-gram
              PositionalQgram matchingPosQgram =  new PositionalQgram( posQgram._qgram, matchingPosition );
              HilbertCountMap hilbertCountMap = firstLevelInvertedIndex.read( matchingPosQgram );

              if ( hilbertCountMap != null )
              {
                intersectionJoinOperator.join( hilbertCountMap._map, intersectingNodeStatsMap );
                approximatePrefixNodeCandidates.addAll( hilbertCountMap._map.keySet() );
              }                            
            }
          }
        }
      }
      
      // if the number of candidate nodes is small, do not need to applied other filters
      if ( approximatePrefixNodeCandidates.size() < visitingNodeSizeThreshold )
      {
        return;
      }      
    
      // else apply other filtering methods for the nodes
      if ( query._minPosQgramMatch >= 1 )
      {             
        // if the text is short, apply the count filter for positional q-gram
        if ( query._queryText.length() - query._smallQValue + 1  <= lamdaValue - query._tau )
        {
          TreeSet< String > approximatePrefixNodeCandidatesForShortText = new TreeSet<String>();      
          
          approximatePrefixNodeFilterForShortText( query, intersectingNodeStatsMap, query._minPosQgramMatch,
            approximatePrefixNodeCandidatesForShortText );                
       
          // join the result
          if ( approximatePrefixNodeCandidatesForShortText.isEmpty() )
          {
            approximatePrefixNodeCandidates.clear();
            return;
          }
          else
          {
            intersectionJoinOperator.join( approximatePrefixNodeCandidates, approximatePrefixNodeCandidatesForShortText );
          }
        }
        
        // if the size id too large, apply the count filter for approximate substring
        if ( approximatePrefixNodeCandidates.size() >= visitingNodeSizeThreshold )
        {
          if ( tokenMinMatchThreshold >= 1 )
          {                                              
            TreeSet<String> approximateSubstringNodeCandidates = new TreeSet<String>();          
            this.approximateSubstringNodeFilter( query, tokenMinMatchThreshold,
                intersectingNodeStatsMap, approximateSubstringNodeCandidates );
  
            // no node having the q-gram count greater than the threshold
            if ( approximateSubstringNodeCandidates.isEmpty() )
            {
              approximatePrefixNodeCandidates.clear();
            }
            else
            {
              intersectionJoinOperator.join( approximatePrefixNodeCandidates, approximateSubstringNodeCandidates );
            }          
          }  
        }
              
      }        
    }
    
    // the query text is long, apply the approximate substring node filter
    else
    {           
      TreeSet<String> approximateSubstringNodeCandidates = new TreeSet<String>();      
      this.approximateSubstringNodeFilter( query, tokenMinMatchThreshold,
          intersectingNodeStatsMap, approximateSubstringNodeCandidates );

      // no node having the q-gram count greater than the threshold
      if ( ! approximateSubstringNodeCandidates.isEmpty() )
      {
        approximatePrefixNodeCandidates.addAll( approximateSubstringNodeCandidates );
      }            
    }    
  }
  
  
  // if query length is short, can apply the position filter
  // use position filter    
  protected void approximatePrefixNodeFilterForShortText( 
      SpatialQuery query, 
      IntersectingNodeStatsMap intersectingNodeStatsMap,
      int posMinMatchThreshold,
      TreeSet< String > approximatePrefixNodeCandidatesForShortText,
      HashMap< PositionalQgram, HilbertCountMap > memPosQgramCountPairInvertedMap )
  {              
    // used to join the intersecting nodes
    IntersectionJoin< Integer, NodeStatistic > intersectionJoinOperator = new IntersectionJoin< Integer, NodeStatistic >();
    
    // overall count map
    HashMap< String, Integer > totalCountMap = new HashMap< String, Integer >();
    
    Iterator<Entry<String, TreeSet<Integer>>> gramItr = query._smallQgramTokenPositionMap.entrySet().iterator();    
    while( gramItr.hasNext() )
    {
      // for every q-gram
      Entry<String, TreeSet<Integer>> entry = gramItr.next();
      String gram = entry.getKey();
      TreeSet<Integer> posList = entry.getValue();
      int bound = posList.size();
      
      // count for the single q-gram
      HashMap< String, Integer > singleGramCountMap = new HashMap< String, Integer > ();
      
      // store the matching positions
      TreeSet<Integer> matchingPosList = new TreeSet<Integer>();      
      Iterator< Integer > posItr = posList.iterator();
      while ( posItr.hasNext() )
      {
        int pos = posItr.next();
        
        for ( int offset = (- query._tau); offset <= query._tau; offset ++ )
        {
          int matchingPos = pos + offset;
          if ( matchingPos >= 0 )
          {
            matchingPosList.add( matchingPos );
          }
        }
      }
      
      for ( int matchingPos : matchingPosList )
      {
        PositionalQgram matchingPosQgram = new PositionalQgram( gram, matchingPos );
        
        HilbertCountMap hilbertCountMap = memPosQgramCountPairInvertedMap.get( matchingPosQgram );

        if ( hilbertCountMap != null )
        {
          // if the list is not empty, the 
          if ( ! hilbertCountMap.isEmpty() )
          {
            // join with the intersecting nodes
            intersectionJoinOperator.join( hilbertCountMap._map, intersectingNodeStatsMap );
            
            
            // count the nodes
            for ( String hilbertCode : hilbertCountMap._map.keySet() )
            {      
              if ( ! approximatePrefixNodeCandidatesForShortText.contains( hilbertCode ) )
              {   
                Integer singleGramCount = singleGramCountMap.get( hilbertCode );
                if ( singleGramCount == null )
                {
                  singleGramCountMap.put( hilbertCode, 1 );
                }
                else
                {
                  // the size can not exceed this bound
                  if ( singleGramCount < bound )
                  {
                    singleGramCountMap.put( hilbertCode, singleGramCount + 1 );
                  }
                }
              }
            }                                                                                                      
          }
        }
        else
        {                   
          // read from the database
          hilbertCountMap = firstLevelInvertedIndex.read( matchingPosQgram );
          if ( hilbertCountMap != null && ! hilbertCountMap.isEmpty() )
          {                         
            memPosQgramCountPairInvertedMap.put( matchingPosQgram, hilbertCountMap );
            
            // join with the intersecting nodes
            intersectionJoinOperator.join( hilbertCountMap._map, intersectingNodeStatsMap );
            
            // count the nodes
            for ( String hilbertCode : hilbertCountMap._map.keySet() )
            { 
              if ( ! approximatePrefixNodeCandidatesForShortText.contains( hilbertCode ) )
              {
                Integer singleGramCount = singleGramCountMap.get( hilbertCode );
                if ( singleGramCount == null )
                {
                  singleGramCountMap.put( hilbertCode, 1 );
                }
                else
                {
                  if ( singleGramCount < bound )
                  {
                    singleGramCountMap.put( hilbertCode, singleGramCount + 1 );
                  }
                }
              }
            }                                          
          }     
          else
          {            
            memPosQgramCountPairInvertedMap.put( matchingPosQgram, new HilbertCountMap() );            
          }
        }        
      }   
      
      // add the singleGramCountMap to the totalCountMap
      Iterator<Entry<String, Integer>> singleMapItr = singleGramCountMap.entrySet().iterator();
      while( singleMapItr.hasNext() )
      {
        Entry<String, Integer> singleEntry = singleMapItr.next();
        String hilbertCode = singleEntry.getKey();
        int singleCount = singleEntry.getValue();
        
        Integer totalCount = totalCountMap.get( hilbertCode );
        if ( totalCount == null )
        {
          totalCountMap.put( hilbertCode, singleCount );          
        }
        else
        {
          totalCountMap.put( hilbertCode, singleCount + totalCount );
        }                        
        singleMapItr.remove();
      }
      
      
      
      // output candidates every round
      Iterator<Entry<String, Integer>> totalMapItr = totalCountMap.entrySet().iterator();
      while( totalMapItr.hasNext() )
      {
        Entry<String, Integer> totalEntry = totalMapItr.next();
        String hilbertCode = totalEntry.getKey();
        int count = totalEntry.getValue();
        
        if ( count >= posMinMatchThreshold )         
        {
          approximatePrefixNodeCandidatesForShortText.add( hilbertCode );
          totalMapItr.remove();
        }
      }
    }  
  }
  
  
  
  // if query length is short, can apply the position filter
  // use position filter    
  protected void approximatePrefixNodeFilterForShortText( 
      SpatialQuery query, 
      IntersectingNodeStatsMap intersectingNodeStatsMap,
      int posMinMatchThreshold,
      TreeSet< String > approximatePrefixNodeCandidatesForShortText )
  {              
    // used to join the intersecting nodes
    IntersectionJoin< Integer, NodeStatistic > intersectionJoinOperator = new IntersectionJoin< Integer, NodeStatistic >();
    
    // overall count map
    HashMap< String, Integer > totalCountMap = new HashMap< String, Integer >();
    
    Iterator<Entry<String, TreeSet<Integer>>> gramItr = query._smallQgramTokenPositionMap.entrySet().iterator();    
    while( gramItr.hasNext() )
    {
      // for every q-gram
      Entry<String, TreeSet<Integer>> entry = gramItr.next();
      String gram = entry.getKey();
      TreeSet<Integer> posList = entry.getValue();
      int bound = posList.size();
      
      // count for the single q-gram
      HashMap< String, Integer > singleGramCountMap = new HashMap< String, Integer > ();
      
      // store the matching positions
      TreeSet<Integer> matchingPosList = new TreeSet<Integer>();      
      Iterator< Integer > posItr = posList.iterator();
      while ( posItr.hasNext() )
      {
        int pos = posItr.next();
        
        for ( int offset = (- query._tau); offset <= query._tau; offset ++ )
        {
          int matchingPos = pos + offset;
          if ( matchingPos >= 0 )
          {
            matchingPosList.add( matchingPos );
          }
        }
      }
      
      for ( int matchingPos : matchingPosList )
      {
        PositionalQgram matchingPosQgram = new PositionalQgram( gram, matchingPos );       
        HilbertCountMap hilbertCountMap = firstLevelInvertedIndex.read( matchingPosQgram );
            
        if ( hilbertCountMap != null && !hilbertCountMap.isEmpty() )
        {
          // join with the intersecting nodes
          intersectionJoinOperator.join( hilbertCountMap._map, intersectingNodeStatsMap );

          // count the nodes
          for ( String hilbertCode : hilbertCountMap._map.keySet() )
          {
            if ( !approximatePrefixNodeCandidatesForShortText.contains( hilbertCode ) )
            {
              Integer singleGramCount = singleGramCountMap.get( hilbertCode );
              if ( singleGramCount == null )
              {
                singleGramCountMap.put( hilbertCode, 1 );
              }
              else
              {
                if ( singleGramCount < bound )
                {
                  singleGramCountMap.put( hilbertCode, singleGramCount + 1 );
                }
              }
            }
          }
        }                
      }   
      
      // add the singleGramCountMap to the totalCountMap
      Iterator<Entry<String, Integer>> singleMapItr = singleGramCountMap.entrySet().iterator();
      while( singleMapItr.hasNext() )
      {
        Entry<String, Integer> singleEntry = singleMapItr.next();
        String hilbertCode = singleEntry.getKey();
        int singleCount = singleEntry.getValue();
        
        Integer totalCount = totalCountMap.get( hilbertCode );
        if ( totalCount == null )
        {
          totalCountMap.put( hilbertCode, singleCount );          
        }
        else
        {
          totalCountMap.put( hilbertCode, singleCount + totalCount );
        }                        
        singleMapItr.remove();
      }
      
            
      // output candidates every round
      Iterator<Entry<String, Integer>> totalMapItr = totalCountMap.entrySet().iterator();
      while( totalMapItr.hasNext() )
      {
        Entry<String, Integer> totalEntry = totalMapItr.next();
        String hilbertCode = totalEntry.getKey();
        int count = totalEntry.getValue();
        
        if ( count >= posMinMatchThreshold )         
        {
          approximatePrefixNodeCandidatesForShortText.add( hilbertCode );
          totalMapItr.remove();
        }
      }
    }  
  }
  
  
  
  
  protected void approximatePrefixPositionalQgramCountFilterInNode(
     SpatialQuery query,
     int posMinMatchThreshold,
     String nodeHilbertCode,
     HashSet<Integer> approximatePrefixObjectCandidate,
     HashMap< PositionalQgram, TreeSet< Integer >> infrequentPosQgramMap,
     HashMap< SecondLevelKey, TreeSet< Integer >> memHilbPosQgramInvertedMap)
  {
    // if query length is short, can apply the position filter, use the count filter for positional q-grams    
    
    // overall count map
    HashMap< Integer, Integer > totalCountMap = new HashMap< Integer, Integer >();
    
    Iterator<Entry<String, TreeSet<Integer>>> gramItr = query._smallQgramTokenPositionMap.entrySet().iterator();    
    while( gramItr.hasNext() )
    {
      // for every q-gram
      Entry<String, TreeSet<Integer>> entry = gramItr.next();
      String gram = entry.getKey();
      TreeSet<Integer> posList = entry.getValue();
      int bound = posList.size();
      
      // count for the single q-gram
      HashMap< Integer, Integer > singleObjectCountMap = new HashMap< Integer, Integer > ();
      
      // store the matching positions, get the matching positional q-grams
      TreeSet<Integer> matchingPosList = new TreeSet<Integer>();      
      Iterator< Integer > posItr = posList.iterator();
      while ( posItr.hasNext() )
      {
        int pos = posItr.next();
        
        for ( int offset = (- query._tau); offset <= query._tau; offset ++ )
        {
          int matchingPos = pos + offset;
          if ( matchingPos >= 0 )
          {
            matchingPosList.add( matchingPos );
          }
        }
      }
      
      // read each matching positional q-gram
      for ( int matchingPos : matchingPosList )
      {
        PositionalQgram matchingPosQgram = new PositionalQgram( gram, matchingPos );
                
        // check whether it is infrequent
        TreeSet< Integer > infrequentIdSet = infrequentPosQgramMap.get( matchingPosQgram );
        
        // read it from the database
        if ( infrequentIdSet == null )
        {          
          infrequentIdSet = infrequentPosQgramInvertedIndex.readIdSet( matchingPosQgram );

          //  save it to memory
          if ( infrequentIdSet == null )
          {
            infrequentPosQgramMap.put( matchingPosQgram, new TreeSet< Integer >() );
          }
          else
          {
            infrequentPosQgramMap.put( matchingPosQgram, infrequentIdSet );
          }
        }
        
        // if the q-gram token is infrequent
        if ( ( infrequentIdSet != null ) && ( ! infrequentIdSet.isEmpty() ) )
        {              
          // count the object
          for ( int objectid : infrequentIdSet )
          {      
            // if the object id does not pass the count filter
            if ( ! approximatePrefixObjectCandidate.contains( objectid ) )
            {   
              Integer singleObjectCount = singleObjectCountMap.get( objectid );
              if ( singleObjectCount == null )
              {
                singleObjectCountMap.put( objectid, 1 );
              }
              else
              {
                if ( singleObjectCount < bound )
                {
                  singleObjectCountMap.put( objectid, singleObjectCount + 1 );
                }
              }
            }
          }    
        } 
        // else the positional q-gram is not infrequent, go to the corresponding node
        else
        {
          SecondLevelKey  secondLevelKey = new SecondLevelKey( nodeHilbertCode, matchingPosQgram );
          
          TreeSet<Integer> objectIdSet = memHilbPosQgramInvertedMap.get( secondLevelKey );

          if ( objectIdSet != null )
          {
            // if the list is not empty, the 
            if ( ! objectIdSet.isEmpty() )
            {                     
              // count the object
              for ( int objectid : objectIdSet )
              {      
                // if the object id does not pass the count filter
                if ( ! approximatePrefixObjectCandidate.contains( objectid ) )
                {   
                  Integer singleObjectCount = singleObjectCountMap.get( objectid );
                  if ( singleObjectCount == null )
                  {
                    singleObjectCountMap.put( objectid, 1 );
                  }
                  else
                  {
                    if ( singleObjectCount < bound )
                    {
                      singleObjectCountMap.put( objectid, singleObjectCount + 1 );
                    }
                  }
                }
              }                                                                                                      
            }
          }
          else
          {                   
            // read from the database
            objectIdSet = secondLevelInvertedIndex.readObjectList( secondLevelKey );
            
            // the objectIdSet can not be reused anymore, so no need to store it
            if ( objectIdSet != null && ! objectIdSet.isEmpty() )
            {          
              memHilbPosQgramInvertedMap.put( secondLevelKey, objectIdSet );
              // count the nodes
              for ( int objectid : objectIdSet )
              { 
                // if the object does not pass the count filter
                if ( ! approximatePrefixObjectCandidate.contains( objectid ) )
                {
                  Integer singleObjectCount = singleObjectCountMap.get( objectid );
                  if ( singleObjectCount == null )
                  {
                    singleObjectCountMap.put( objectid, 1 );
                  }
                  else
                  {
                    if ( singleObjectCount < bound )
                    {
                      singleObjectCountMap.put( objectid, singleObjectCount + 1 );
                    }
                  }
                }
              }                                       
            }  
            else 
            {         
              memHilbPosQgramInvertedMap.put( secondLevelKey, new TreeSet< Integer >() );               
            }            
          } 
        }            
      }   
      
      // add the singleGramCountMap to the totalCountMap
      Iterator<Entry<Integer, Integer>> singleMapItr = singleObjectCountMap.entrySet().iterator();
      while( singleMapItr.hasNext() )
      {
        Entry<Integer, Integer> singleEntry = singleMapItr.next();
        int objectid = singleEntry.getKey();
        int singleCount = singleEntry.getValue();
        
        Integer totalCount = totalCountMap.get( objectid );
        if ( totalCount == null )
        {
          totalCountMap.put( objectid, singleCount );          
        }
        else
        {
          totalCountMap.put( objectid, singleCount + totalCount );
        }                        
        singleMapItr.remove();
      }
      
      
      // output candidates every round
      Iterator<Entry<Integer, Integer>> totalMapItr = totalCountMap.entrySet().iterator();
      while( totalMapItr.hasNext() )
      {
        Entry<Integer, Integer> totalEntry = totalMapItr.next();
        int objectid = totalEntry.getKey();
        int count = totalEntry.getValue();
        
        if ( count >= posMinMatchThreshold )         
        {
          approximatePrefixObjectCandidate.add( objectid );
          totalMapItr.remove();
        }
      }
    }  
  }
  
  
  

  protected void approximatePrefixPositionalQgramCountFilterInNode(
     SpatialQuery query,
     int posMinMatchThreshold,
     String nodeHilbertCode,
     HashSet<Integer> approximatePrefixObjectCandidate )
  {
    // if query length is short, can apply the position filter, use the count filter for positional q-grams    
    
    // overall count map
    HashMap< Integer, Integer > totalCountMap = new HashMap< Integer, Integer >();
    
    Iterator<Entry<String, TreeSet<Integer>>> gramItr = query._smallQgramTokenPositionMap.entrySet().iterator();    
    while( gramItr.hasNext() )
    {
      // for every q-gram
      Entry<String, TreeSet<Integer>> entry = gramItr.next();
      String gram = entry.getKey();
      TreeSet<Integer> posList = entry.getValue();
      int bound = posList.size();
      
      // count for the single q-gram
      HashMap< Integer, Integer > singleObjectCountMap = new HashMap< Integer, Integer > ();
      
      // store the matching positions, get the matching positional q-grams
      TreeSet<Integer> matchingPosList = new TreeSet<Integer>();      
      Iterator< Integer > posItr = posList.iterator();
      while ( posItr.hasNext() )
      {
        int pos = posItr.next();
        
        for ( int offset = (- query._tau); offset <= query._tau; offset ++ )
        {
          int matchingPos = pos + offset;
          if ( matchingPos >= 0 )
          {
            matchingPosList.add( matchingPos );
          }
        }
      }
      
      // read each matching positional q-gram
      for ( int matchingPos : matchingPosList )
      {
        PositionalQgram matchingPosQgram = new PositionalQgram( gram, matchingPos );
                
        // check whether it is infrequent
        TreeSet< Integer > infrequentIdSet = infrequentPosQgramInvertedIndex.readIdSet( matchingPosQgram );
        
                
        // if the q-gram token is infrequent
        if ( ( infrequentIdSet != null ) && ( ! infrequentIdSet.isEmpty() ) )
        {              
          // count the object
          for ( int objectid : infrequentIdSet )
          {      
            // if the object id does not pass the count filter
            if ( ! approximatePrefixObjectCandidate.contains( objectid ) )
            {   
              Integer singleObjectCount = singleObjectCountMap.get( objectid );
              if ( singleObjectCount == null )
              {
                singleObjectCountMap.put( objectid, 1 );
              }
              else
              {
                if ( singleObjectCount < bound )
                {
                  singleObjectCountMap.put( objectid, singleObjectCount + 1 );
                }
              }
            }
          }    
        } 
        // else the positional q-gram is not infrequent, go to the corresponding node
        else
        {
          SecondLevelKey  secondLevelKey = new SecondLevelKey( nodeHilbertCode, matchingPosQgram );         
          TreeSet<Integer> objectIdSet = secondLevelInvertedIndex.readObjectList( secondLevelKey );
                                                 
          
          if ( objectIdSet != null && !objectIdSet.isEmpty() )
          {
            for ( int objectid : objectIdSet )
            {
              // if the object does not pass the count filter
              if ( !approximatePrefixObjectCandidate.contains( objectid ) )
              {
                Integer singleObjectCount = singleObjectCountMap.get( objectid );
                if ( singleObjectCount == null )
                {
                  singleObjectCountMap.put( objectid, 1 );
                }
                else
                {
                  if ( singleObjectCount < bound )
                  {
                    singleObjectCountMap.put( objectid, singleObjectCount + 1 );
                  }
                }
              }
            }
          }                                  
        }            
      }   
      
      // add the singleGramCountMap to the totalCountMap
      Iterator<Entry<Integer, Integer>> singleMapItr = singleObjectCountMap.entrySet().iterator();
      while( singleMapItr.hasNext() )
      {
        Entry<Integer, Integer> singleEntry = singleMapItr.next();
        int objectid = singleEntry.getKey();
        int singleCount = singleEntry.getValue();
        
        Integer totalCount = totalCountMap.get( objectid );
        if ( totalCount == null )
        {
          totalCountMap.put( objectid, singleCount );          
        }
        else
        {
          totalCountMap.put( objectid, singleCount + totalCount );
        }                        
        singleMapItr.remove();
      }
      
      
      // output candidates every round
      Iterator<Entry<Integer, Integer>> totalMapItr = totalCountMap.entrySet().iterator();
      while( totalMapItr.hasNext() )
      {
        Entry<Integer, Integer> totalEntry = totalMapItr.next();
        int objectid = totalEntry.getKey();
        int count = totalEntry.getValue();
        
        if ( count >= posMinMatchThreshold )         
        {
          approximatePrefixObjectCandidate.add( objectid );
          totalMapItr.remove();
        }
      }
    }  
  }
  

  private void approximatePrefixQueryInSingleNode(
      SpatialQuery query,
      HashMap< Integer, SimpleEntry < QueryType, SpatialObject >> resultMap,
      String nodeHilbertCode,
      NodeStatistic nodeStats,      
      QgramFilter qgramFilter,
      MergeSkip mergeSkipOperator )
  { 
         
    // only few objects in the node, directly check the result
    if ( nodeStats._intersectingObjectSet.size() <= sparseThreshold )
    {
      for ( int objectid : nodeStats._intersectingObjectSet )
      {        
        SpatialObject object = objectDatabase.getSpatialObject( objectid );

        // if pass the q-gram filter
        if ( qgramFilter.approximatePrefixFilter( 
              query._queryText, query._largeQgramTokenPositionMap, query._largeQgramPrefixFilterTokenPositionMap,
              object._text, query._tau ) ) 
        {                 
          // if pass the string verification
          if ( StringVerification.isTauPrefix( query._queryText, object._text, query._tau ) )
          {
            resultMap.put( objectid, new SimpleEntry<QueryType, SpatialObject>(QueryType.APPROXIMATE_PREFIX_RANGE, object ) );
          }
        }
      }
    }

    // need to retrieve the inverted index to do the join operation
    else
    {
      HashSet < Integer > approximatePrefixCandidatesInNode = new HashSet< Integer >();
      boolean applyCountFilterForPositioinalQgram = false;
      
      // prefixFilter of object
      if ( query._queryText.length() <= 20 )
      {        
        // newly change
        int prefixFilterLength = query._tau * query._smallQValue;                
        
        for ( PositionalQgram posQgram : query._firstMPositionalQgramSet )
        {
          if ( posQgram._pos <= prefixFilterLength )
          {
            // get the matching positional q-grams
            for ( int i = -query._tau; i <= query._tau; i++ )
            {
              int matchingPosition = posQgram._pos + i;
              
              if ( matchingPosition >= 0 && matchingPosition <= prefixFilterLength )
              {
                // read infrequent q-gram first
                PositionalQgram matchingPosQgram = new PositionalQgram( posQgram._qgram , matchingPosition);
                TreeSet< Integer > idSet = infrequentPosQgramInvertedIndex.readIdSet( matchingPosQgram );
                                                
                if ( idSet != null && ! idSet.isEmpty()  )
                {
                  approximatePrefixCandidatesInNode.addAll( idSet );  
                }
                // read the second level inverted index
                else
                {                  
                  SecondLevelKey hilbertPosQgram = new SecondLevelKey(nodeHilbertCode, matchingPosQgram );                                    
                  idSet = secondLevelInvertedIndex.readObjectList( hilbertPosQgram );
                                                    
                  if ( idSet != null )
                  {                  
                    // do the prefix filter, the prefix filter ensures that objects must have one matching positional q-gram
                    approximatePrefixCandidatesInNode.addAll( idSet );  
                  }
                }                   
              }
            }
          }
        }
        
        // prune the non-intersecting objects
        approximatePrefixCandidatesInNode.retainAll( nodeStats._intersectingObjectSet );              
     
        // if the candidate size is large, do extra object filter...
        if ( approximatePrefixCandidatesInNode.size() > sparseThreshold )
        {              
          // if the text is short, apply the count filter for positional q-gram
          if ( query._minPosQgramMatch >= 1 )
          {     
            // if the length of the query text is short enough
            if ( query._queryText.length() - query._smallQValue + 1  <= lamdaValue - query._tau )
            {       
              applyCountFilterForPositioinalQgram = true;
              HashSet<Integer> approximatePrefixObjectForCountFilter = new HashSet<Integer> ();
              
              // get the count filter
             approximatePrefixPositionalQgramCountFilterInNode( 
                  query, query._minPosQgramMatch, nodeHilbertCode, 
                  approximatePrefixObjectForCountFilter );  
                            
              
              // retain the result with the approximate substring candidates
              if ( approximatePrefixObjectForCountFilter.isEmpty() )
              {
                approximatePrefixCandidatesInNode.clear();
              }
              else
              {
                approximatePrefixCandidatesInNode.retainAll( approximatePrefixObjectForCountFilter );
              }               
            }          
            
            // if the candidate size is large, do q-gram count filter
            
            if ( approximatePrefixCandidatesInNode.size() > sparseThreshold )
            {
              if ( query._minQgramTokenMatch >= 1 )
              {
                HashSet< Integer > approximateSubstringCandidatesInNode = new HashSet< Integer > ();
                approximateSubstringQgramCountFilterAtNode( query, query._minQgramTokenMatch,
                  nodeHilbertCode, mergeSkipOperator, approximateSubstringCandidatesInNode); 
                
                // retain the result with the approximate substring candidates
                if ( approximateSubstringCandidatesInNode.isEmpty() )
                {
                  approximatePrefixCandidatesInNode.clear();
                }
                else
                {
                  approximatePrefixCandidatesInNode.retainAll( approximateSubstringCandidatesInNode );
                }   
              }         
            }
            
          }     
        }    
      }
      
      // else the query text is very long, the prefix filter can not be applied, use the approximate substring filter.
      else
      {
        HashSet< Integer > approximateSubstringCandidatesInNode = new HashSet< Integer > ();
        
        this.approximateSubstringQgramCountFilterAtNode( query, query._minQgramTokenMatch,
            nodeHilbertCode, mergeSkipOperator, approximateSubstringCandidatesInNode ); 
        
        // retain the result with the approximate substring candidates
        if ( ! approximateSubstringCandidatesInNode.isEmpty() )
        {
          // add the approximate substring candidate
          approximatePrefixCandidatesInNode.addAll( approximateSubstringCandidatesInNode );
          
          // prune the non-intersecting objects
          approximatePrefixCandidatesInNode.retainAll( nodeStats._intersectingObjectSet );      
        } 
        
      }
      
      // verify each object candidate                
      if ( ! approximatePrefixCandidatesInNode.isEmpty() )
      {
        for ( int objectid : approximatePrefixCandidatesInNode )
        {
                    
          SpatialObject object = objectDatabase.getSpatialObject( objectid );

          // if do the count filer for positional q-gram before
          if ( applyCountFilterForPositioinalQgram )
          {           
            if ( StringVerification.isTauPrefix( query._queryText, object._text, query._tau ) )
            {
              resultMap.put( objectid, new SimpleEntry< QueryType, SpatialObject >(
                  QueryType.APPROXIMATE_PREFIX_RANGE, object ) );
            }
          }
          // else if it passes the q-gram
          else if ( qgramFilter.approximatePrefixFilter( 
              query._queryText, query._largeQgramTokenPositionMap, 
              query._largeQgramPrefixFilterTokenPositionMap, object._text, query._tau ) )
          {
            if ( StringVerification.isTauPrefix( query._queryText, object._text, query._tau ) )
            {
              resultMap.put( objectid, new SimpleEntry< QueryType, SpatialObject >(
                  QueryType.APPROXIMATE_PREFIX_RANGE, object ) );
            }
          }                      
        }
      }         
    }
  }    
  
  
  
  
  public HashMap< Integer, SimpleEntry < QueryType, SpatialObject >> approSubstringQueryOnly( SpatialQuery query )
  {    
    HashMap< Integer, SimpleEntry < QueryType, SpatialObject > > results = new HashMap< Integer, SimpleEntry < QueryType, SpatialObject > >();             
    
    if ( query._queryText.length() >= 6 )
    {
      // approximate substring query in the specific query region
      query._queryType = QueryType.APPROXIMATE_SUBSTRING_RANGE;               
                                   
      
      // get the intersecting node statistics        
      IntersectingNodeStatsMap intersectingNodeStatsMap = new IntersectingNodeStatsMap();
      quadtree.getIntersectingNodeStatistic( query._queryRegion, intersectingNodeStatsMap, sparseThreshold );           
      
      // approximate prefix query in the given spatial range                                        
      QgramFilter qgramFilter = new QgramFilter ( query._largeQgramGenerator );  
      MergeSkip mergeSkipOperator = new MergeSkip ();
      
      
      this.approximateSubstringRangeQuery( query, results, query._minQgramTokenMatch,  
        intersectingNodeStatsMap, qgramFilter, mergeSkipOperator );
      
    }
    return results;
  }
  

  public void approximateSubstringRangeQuery( SpatialQuery query,
      int tokenMinMatchThreshold,
      IntersectingNodeStatsMap intersectingNodeStatsMap, 
      HashSet< Integer > approximateSubstringResult,
      BooleanObject doApproximateSubstringNodeFilter,
      TreeSet< String > approximateSubstringNodeCandidates,
      HashMap< Integer, SpatialObject > memObjectMap,
      HashMap< String, TreeSet< Integer >> memInfreqQgramTokenInvertedMap,
      HashMap< String, HilbertCountMap > memQgramTokenCountPairInvertedMap,
      HashMap< String, TreeSet< Integer >> memHilbQgramTokenInvertedMap,
      QgramFilter qgramFilter,
      MergeSkip mergeSkipOperator,
      HashMap< String, Collection< Integer >> approximateSubstringCandidateMap)
  {
    // do need to do node filter
    if ( intersectingNodeStatsMap.denseNodeNumber < visitingNodeSizeThreshold )
    {
      Iterator< Entry< String, NodeStatistic >> itr = intersectingNodeStatsMap.entrySet().iterator();      
      while ( itr.hasNext() )
      {
        Entry< String, NodeStatistic > entry = itr.next();
        String nodeHilbertCode = entry.getKey();
        NodeStatistic nodeStats = entry.getValue();
        
        this.approximateSubstringQueryInSingleNode( query, approximateSubstringResult, nodeHilbertCode, nodeStats,
          memObjectMap, memInfreqQgramTokenInvertedMap, memHilbQgramTokenInvertedMap, 
          qgramFilter, mergeSkipOperator, approximateSubstringCandidateMap );
      }
    }
    // node filter
    else
    {
      // do approximate substring node filter
      this.approximateSubstringNodeFilter( 
          query, tokenMinMatchThreshold, intersectingNodeStatsMap, doApproximateSubstringNodeFilter, 
          approximateSubstringNodeCandidates, memQgramTokenCountPairInvertedMap );   
     
      // object process in each candidate node
      for ( String nodeHilbertCode : approximateSubstringNodeCandidates )
      {
        // get the node statistic
        NodeStatistic nodeStats = intersectingNodeStatsMap.get( nodeHilbertCode );      
        
        this.approximateSubstringQueryInSingleNode( query, approximateSubstringResult, nodeHilbertCode, nodeStats, 
          memObjectMap, memInfreqQgramTokenInvertedMap, memHilbQgramTokenInvertedMap, 
          qgramFilter, mergeSkipOperator, approximateSubstringCandidateMap );
      } 
    }      
  }
  
  
  

  public void approximateSubstringRangeQuery( 
      SpatialQuery query,
      HashMap< Integer, SimpleEntry < QueryType, SpatialObject > > results,
      int tokenMinMatchThreshold,
      IntersectingNodeStatsMap intersectingNodeStatsMap, 
      QgramFilter qgramFilter,
      MergeSkip mergeSkipOperator)
  {
    // do need to do node filter
    if ( intersectingNodeStatsMap.denseNodeNumber < visitingNodeSizeThreshold )
    {
      Iterator< Entry< String, NodeStatistic >> itr = intersectingNodeStatsMap.entrySet().iterator();      
      while ( itr.hasNext() )
      {
        Entry< String, NodeStatistic > entry = itr.next();
        String nodeHilbertCode = entry.getKey();
        NodeStatistic nodeStats = entry.getValue();
        
        approximateSubstringQueryInSingleNode( query, results, nodeHilbertCode, nodeStats, qgramFilter, mergeSkipOperator );
      }
    }
    // node filter
    else
    {
      // do approximate substring node filter
      TreeSet<String> approximateSubstringNodeCandidates = new TreeSet<String>();                   
      this.approximateSubstringNodeFilter( query, tokenMinMatchThreshold,
          intersectingNodeStatsMap, approximateSubstringNodeCandidates );
      
      
      // object process in each candidate node
      for ( String nodeHilbertCode : approximateSubstringNodeCandidates )
      {
        // get the node statistic
        NodeStatistic nodeStats = intersectingNodeStatsMap.get( nodeHilbertCode );      
        
        approximateSubstringQueryInSingleNode( query, results, nodeHilbertCode, nodeStats, qgramFilter, mergeSkipOperator );       
      } 
    }      
  }
  
  
  protected void approximateSubstringNodeFilter( 
      SpatialQuery query,
      int tokenMinMatchThreshold,
      IntersectingNodeStatsMap intersectingNodeStatsMap,
      BooleanObject doApproximateSubstringNodeFilter,
      TreeSet<String> approximateSubstringNodeCandidates,      
      HashMap< String, HilbertCountMap > memQgramTokenCountPairInvertedMap )
  {
    if( doApproximateSubstringNodeFilter.isTrue() )
    {
      return;
    }    
    doApproximateSubstringNodeFilter.setTrue();    
     
    IntersectionJoin< Integer, NodeStatistic > intersectionJoinOperator = new IntersectionJoin< Integer, NodeStatistic >();
    
    ArrayList< NavigableMap< String, Integer >> sortedMapList = new ArrayList< NavigableMap< String, Integer >> ();
    ArrayList< Integer > weightList = new ArrayList< Integer > ();
    
    Iterator< Entry< String, TreeSet< Integer >>> tokenItr = query._largeQgramTokenPositionMap.entrySet().iterator();        
    while ( tokenItr.hasNext() )
    {
      Entry< String, TreeSet< Integer > > entry = tokenItr.next();
      String token = entry.getKey();
      int queryCount = entry.getValue().size();      
      
      HilbertCountMap hilbertCountMap = memQgramTokenCountPairInvertedMap.get( token );
                       
      if ( hilbertCountMap != null )
      {
        if ( hilbertCountMap.isEmpty() )
        {
          continue;
        }
      }
      else
      {       
        hilbertCountMap = qgramTokenCountPairInvertedIndex.read( token );
        if ( hilbertCountMap == null )
        {          
          memQgramTokenCountPairInvertedMap.put( token, new HilbertCountMap() );
          continue;
        }
        else 
        {
          memQgramTokenCountPairInvertedMap.put( token, hilbertCountMap );
        }
      }
      
      intersectionJoinOperator.join( hilbertCountMap._map, intersectingNodeStatsMap );
      
      // merge skip join
      if ( ! hilbertCountMap.isEmpty() )
      {
        sortedMapList.add( hilbertCountMap._map );
        weightList.add( queryCount );
      }  
    }
        
    // mergeSkip join for node
    MergeSkip countJoinOperator = new MergeSkip();
    countJoinOperator.nodeJoin( sortedMapList, weightList, tokenMinMatchThreshold, approximateSubstringNodeCandidates );
  }
  
  
  protected void approximateSubstringNodeFilter( 
      SpatialQuery query, 
      int tokenMinMatchThreshold,
      IntersectingNodeStatsMap intersectingNodeStatsMap,     
      TreeSet< String > approximateSubstringNodeCandidates )
  {
    IntersectionJoin< Integer, NodeStatistic > intersectionJoinOperator = new IntersectionJoin< Integer, NodeStatistic >();

    ArrayList< NavigableMap< String, Integer >> sortedMapList = new ArrayList< NavigableMap< String, Integer >>();
    ArrayList< Integer > weightList = new ArrayList< Integer >();

    Iterator< Entry< String, TreeSet< Integer >>> tokenItr =
        query._largeQgramTokenPositionMap.entrySet().iterator();
    while ( tokenItr.hasNext() )
    {
      Entry< String, TreeSet< Integer > > entry = tokenItr.next();
      String token = entry.getKey();
      int queryCount = entry.getValue().size();

      HilbertCountMap hilbertCountMap = qgramTokenCountPairInvertedIndex.read( token );       
      if ( hilbertCountMap == null )
      {       
        continue;
      }            
      intersectionJoinOperator.join( hilbertCountMap._map, intersectingNodeStatsMap );

      // merge skip join
      if ( !hilbertCountMap.isEmpty() )
      {
        sortedMapList.add( hilbertCountMap._map );
        weightList.add( queryCount );
      }
    }

    // mergeSkip join for node
    MergeSkip countJoinOperator = new MergeSkip();
    countJoinOperator.nodeJoin( sortedMapList, weightList, tokenMinMatchThreshold,
        approximateSubstringNodeCandidates );
  }

  
  private void approximateSubstringQueryInSingleNode(
     SpatialQuery query,
     HashSet< Integer > approximateSubstringResult,
     String nodeHilbertCode,
     NodeStatistic nodeStats,
     HashMap< Integer, SpatialObject > memObjectMap,
     HashMap< String, TreeSet< Integer >> memInfreqQgramTokenInvertedMap,
     HashMap< String, TreeSet< Integer >> memHilbQgramTokenInvertedMap,
     QgramFilter qgramFilter,
     MergeSkip mergeSkipOperator,
     HashMap< String, Collection< Integer >> approximateSubstringCandidateMap)
  {
    if ( nodeStats._intersectingObjectSet.size() <= sparseThreshold )
    {
      for ( int objectid : nodeStats._intersectingObjectSet )
      {
        if ( ! approximateSubstringResult.contains( objectid ) )
        {
          SpatialObject object = memObjectMap.get( objectid );
          if ( object == null )
          {
            object = objectDatabase.getSpatialObject( objectid );
            memObjectMap.put( objectid, object );
          }
          
          // if pass the q-gram filter
          TreeSet< Integer > startPositionCandidates = new TreeSet< Integer > ();
          
          if ( qgramFilter.approximateSubstringFilter( query._queryText, 
              query._largeQgramTokenPositionMap, object._text, startPositionCandidates, query._tau ))
          {
            // if pass the string verification
            if ( StringVerification.isTauSubstring( query._queryText, object._text, query._tau, startPositionCandidates ))            
            {
              approximateSubstringResult.add( objectid );
            }
          }          
          startPositionCandidates.clear();
        }
      }
    }

    else
    {                  
      Collection< Integer > approximateSubstringCandidateInNode = approximateSubstringCandidateMap.get( nodeHilbertCode );
      
      // if approximateSubstringCandidateInNode do not contains the nodeHilertCode, it means that the inverted index is never visited before
      if ( approximateSubstringCandidateInNode == null )
      {
        approximateSubstringCandidateInNode = new ArrayList < Integer > ();
        this.approximateSubstringQgramCountFilterAtNode( query, query._minQgramTokenMatch, nodeHilbertCode, 
            memInfreqQgramTokenInvertedMap, memHilbQgramTokenInvertedMap, mergeSkipOperator, 
            approximateSubstringCandidateInNode, approximateSubstringCandidateMap );
      }
     
                 
      if ( ! approximateSubstringCandidateInNode.isEmpty() )
      {
        // prune the non-intersecting objects
        approximateSubstringCandidateInNode.retainAll( nodeStats._intersectingObjectSet );      
                
        for ( int objectid : approximateSubstringCandidateInNode )
        {
          if ( ! approximateSubstringResult.contains( objectid ) )
          {
                       
              SpatialObject object = memObjectMap.get( objectid );
              if ( object == null )
              {
                object = objectDatabase.getSpatialObject( objectid );
                memObjectMap.put( objectid, object );
              }
              
              // if pass the q-gram filter
              TreeSet< Integer > startPositionCandidates = new TreeSet< Integer > ();
              
              if ( qgramFilter.approximateSubstringFilterWithoutCountFilter( query._queryText, 
                  query._largeQgramTokenPositionMap, object._text, startPositionCandidates, query._tau ))
              {
                // if pass the string verification
                if ( StringVerification.isTauSubstring( query._queryText, object._text, query._tau, startPositionCandidates ))            
                {
                  approximateSubstringResult.add( objectid );
                }
              }   
              startPositionCandidates.clear();
            
          }                             
        }
      }         
    }
  }
  
  
  private void approximateSubstringQueryInSingleNode( 
      SpatialQuery query,
      HashMap< Integer, SimpleEntry < QueryType, SpatialObject >> resultMap,
      String nodeHilbertCode,
      NodeStatistic nodeStats,
      QgramFilter qgramFilter,
      MergeSkip mergeSkipOperator )
  {
    // directly retrieve spatial objects
    if ( nodeStats._intersectingObjectSet.size() <= sparseThreshold )
    {
      for ( int objectid : nodeStats._intersectingObjectSet )
      {               
        SpatialObject object = objectDatabase.getSpatialObject( objectid );
        TreeSet< Integer > startPositionCandidates = new TreeSet< Integer >();

        // if pass the q-gram filter
        if ( qgramFilter.approximateSubstringFilter( query._queryText,
            query._largeQgramTokenPositionMap, object._text, startPositionCandidates, query._tau ) )
        {
          // if pass the string verification
          if ( StringVerification.isTauSubstring( query._queryText, object._text, query._tau, startPositionCandidates ) )
          {
            resultMap.put( objectid, new SimpleEntry< QueryType, SpatialObject >( QueryType.APPROXIMATE_SUBSTRING_RANGE, object ) );
          }
        }
        startPositionCandidates.clear();

      }
    }
    else
    { 
      // apply count filter
      ArrayList< Integer > approximateSubstringCandidateInNode = new ArrayList< Integer >();        
      this.approximateSubstringQgramCountFilterAtNode( query, query._minQgramTokenMatch,
            nodeHilbertCode, mergeSkipOperator, approximateSubstringCandidateInNode );
        
      if ( !approximateSubstringCandidateInNode.isEmpty() )
      {
     // prune the non-intersecting objects
        approximateSubstringCandidateInNode.retainAll( nodeStats._intersectingObjectSet );      
        
        for ( int objectid : approximateSubstringCandidateInNode )
        {       
            SpatialObject object = objectDatabase.getSpatialObject( objectid );
            TreeSet< Integer > startPositionCandidates = new TreeSet< Integer >();

            // if pass the q-gram filter
            if ( qgramFilter.approximateSubstringFilterWithoutCountFilter( query._queryText,
                query._largeQgramTokenPositionMap, object._text, startPositionCandidates, query._tau ) )
            {
              // if pass the string verification
              if ( StringVerification.isTauSubstring( query._queryText, object._text, query._tau, startPositionCandidates ) )
              {
                resultMap.put( objectid, new SimpleEntry< QueryType, SpatialObject >( QueryType.APPROXIMATE_SUBSTRING_RANGE, object ) );
              }
            }
            startPositionCandidates.clear();        
        }
      }
    }
  }
  
  
  
  protected void approximateSubstringQgramCountFilterAtNode( 
      SpatialQuery query,
      int tokenMinMatchThreshold,
      String nodeHilbertCode, 
      HashMap< String, TreeSet< Integer >> memInfreqQgramTokenInvertedMap,
      HashMap< String, TreeSet< Integer >> memHilbQgramTokenInvertedMap,
      MergeSkip mergeSkipJoin,
      Collection< Integer > candidates,
      HashMap< String, Collection< Integer >> approximateSubstringCandidateMap )
  {
    Iterator< Entry< String, TreeSet< Integer >>> itr = query._largeQgramTokenPositionMap.entrySet().iterator();    
    ArrayList< NavigableSet< Integer >> joinList = new ArrayList< NavigableSet< Integer >>(); 
    ArrayList< Integer > weightList = new ArrayList< Integer >();
    
    while ( itr.hasNext() )
    {
      Entry< String, TreeSet< Integer >> entry = itr.next();
      String token = entry.getKey();
      int tokenCount = entry.getValue().size();            
      
      // check the infrequent q-gram
      TreeSet< Integer > idSet = memInfreqQgramTokenInvertedMap.get( token );
      
      // read it from the database
      if ( idSet == null )
      {
        idSet = infrequentTokenInvertedIndex.getIdSet( token );

        //  save it to memory
        if ( idSet == null )
        {
          memInfreqQgramTokenInvertedMap.put( token, new TreeSet< Integer >() );
        }
        else
        {
          memInfreqQgramTokenInvertedMap.put( token, idSet );
        }
      }
      
      // if the infrequent token contains the q-gram token
      if ( idSet != null && ! idSet.isEmpty() )
      {              
        joinList.add( idSet );
        weightList.add( tokenCount );
      } 
      // else read the second level inverted index
      else
      {
        // second level key
        String hilbertToken = nodeHilbertCode + "," + token;               
        
        // read from the memory
        idSet = memHilbQgramTokenInvertedMap.get( hilbertToken );

        // read it from the database
        if ( idSet == null )
        {
          idSet = hilbertQgramTokenInvertedIndex.getIdSet( hilbertToken );
          
          // put it into memory
          if( idSet != null )
          {
            memHilbQgramTokenInvertedMap.put( hilbertToken, idSet );
          }
          else
          {
            memHilbQgramTokenInvertedMap.put( hilbertToken, new TreeSet<Integer>() );
          }
        }
        
        // accumulate the ( object, matching q-gram count ) pair
        if ( idSet != null && ! idSet.isEmpty() )
        {             
          joinList.add( idSet );
          weightList.add( tokenCount );
        }                        
      }                  
    }  
    
    mergeSkipJoin.idJoin( joinList, weightList, tokenMinMatchThreshold, candidates );    
    approximateSubstringCandidateMap.put( nodeHilbertCode, candidates );    
  }
  
  
  
  protected void approximateSubstringQgramCountFilterAtNode( 
      SpatialQuery query,
      int tokenMinMatchThreshold, 
      String nodeHilbertCode,
      MergeSkip mergeSkipJoin,
      Collection< Integer > candidates )
  {
    Iterator< Entry< String, TreeSet< Integer >>> itr =
        query._largeQgramTokenPositionMap.entrySet().iterator();
    ArrayList< NavigableSet< Integer >> joinList = new ArrayList< NavigableSet< Integer >>();
    ArrayList< Integer > weightList = new ArrayList< Integer >();

    while ( itr.hasNext() )
    {
      Entry< String, TreeSet< Integer >> entry = itr.next();
      String token = entry.getKey();
      int tokenCount = entry.getValue().size();

      // check the infrequent q-gram
      TreeSet< Integer > idSet = infrequentTokenInvertedIndex.getIdSet( token );

      // if the infrequent token contains the q-gram token
      if ( idSet != null && !idSet.isEmpty() )
      {
        joinList.add( idSet );
        weightList.add( tokenCount );
      }
      // else read the second level inverted index
      else
      {
        String hilbertToken = nodeHilbertCode + "," + token;
        idSet = hilbertQgramTokenInvertedIndex.getIdSet( hilbertToken );       

        // accumulate the ( object, matching q-gram count ) pair
        if ( idSet != null && !idSet.isEmpty() )
        {
          joinList.add( idSet );
          weightList.add( tokenCount );
        }
      }
    }

    mergeSkipJoin.idJoin( joinList, weightList, tokenMinMatchThreshold, candidates );    
  }


  private TreeSet< Integer > convertIntArrayToTreeSet( int[] integerArray )
  {
    TreeSet< Integer > set = new TreeSet< Integer >();
    for ( int pos : integerArray )
    {
      set.add( pos );
    }
    return set;
  }

  
  
  private Integer getMinimumValue( ArrayList<Integer> list )
  {
    int min = Integer.MAX_VALUE;
    for ( int value : list )
    {
      if ( value < min )
      {
        min = value;
      }
    }
    return min;
  }
  private void approximatePrefixQueryInSingleNode(
	      SpatialQuery query,
	      HashSet< Integer > approximatePrefixResult,
	      String nodeHilbertCode,
	      NodeStatistic nodeStats,
	      HashMap< Integer, SpatialObject > memObjectMap,
	      HashMap< PositionalQgram, TreeSet< Integer >> memInfreqPosQgramInvertedMap,
	      HashMap< String, TreeSet< Integer >> memInfreqQgramTokenInvertedMap,
	      HashMap< SecondLevelKey, TreeSet< Integer >> memHilbPosQgramInvertedMap,
	      HashMap< String, TreeSet< Integer >> memHilbQgramTokenInvertedMap,
	      QgramFilter qgramFilter,
	      MergeSkip mergeSkipOperator,
	      HashMap< String, Collection< Integer >> approximateSubstringCandidateMap )
	  {    
	    
	    // only few objects in the node, directly check the result
	    if ( nodeStats._intersectingObjectSet.size() <= sparseThreshold )
	    {
	      for ( int objectid : nodeStats._intersectingObjectSet )
	      {
	        if ( ! approximatePrefixResult.contains( objectid ) )
	        {
	          SpatialObject object = memObjectMap.get( objectid );
	          if ( object == null )
	          {
	            object = objectDatabase.getSpatialObject( objectid );            
	            memObjectMap.put( objectid, object );
	          }                        
	          
	          // if pass the q-gram
	          if ( qgramFilter.approximatePrefixFilter( query._queryText, 
	              query._largeQgramTokenPositionMap, query._largeQgramPrefixFilterTokenPositionMap, object._text, query._tau ))
	          {
	            // if pass the string verification
	            if ( StringVerification.isTauPrefix( query._queryText, object._text, query._tau ) )
	            {
	              approximatePrefixResult.add( objectid );
	            }
	          }
	        }
	      }
	    }

	    // need to retrieve the inverted index to do the join operation
	    else
	    {
	      HashSet < Integer > approximatePrefixCandidatesInNode = new HashSet< Integer >();
	      boolean applyCountFilterForPositioinalQgram = false;
	      
	      // prefixFilter of object
	      if ( query._queryText.length() <= 20 )
	      {        
	        // newly change
	        int prefixFilterLength = query._tau * query._smallQValue;                
	        
	        for ( PositionalQgram posQgram : query._firstMPositionalQgramSet )
	        {
	          if ( posQgram._pos <= prefixFilterLength )
	          {
	            // get the matching positional q-grams
	            for ( int i = -query._tau; i <= query._tau; i++ )
	            {
	              int matchingPosition = posQgram._pos + i;
	              
	              if ( matchingPosition >= 0 && matchingPosition <= prefixFilterLength )
	              {
	                // read infrequent q-gram first
	                PositionalQgram matchingPosQgram = new PositionalQgram( posQgram._qgram , matchingPosition);
	                TreeSet< Integer > idSet = memInfreqPosQgramInvertedMap.get( matchingPosQgram );
	                
	                // if the positional q-gram is never visited before, check the disk
	                if ( idSet == null )
	                {
	                  idSet = infrequentPosQgramInvertedIndex.readIdSet( matchingPosQgram );
	                  
	                  // save it to memory
	                  if ( idSet == null )
	                  {
	                    memInfreqPosQgramInvertedMap.put( matchingPosQgram, new TreeSet< Integer >() );
	                  }
	                  else
	                  {
	                    memInfreqPosQgramInvertedMap.put( matchingPosQgram, idSet );
	                  }                 
	                }
	                
	                
	                if ( idSet != null && ! idSet.isEmpty()  )
	                {
	                  approximatePrefixCandidatesInNode.addAll( idSet );  
	                }
	                // read the second level inverted index
	                else
	                {                  
	                  SecondLevelKey hilbertPosQgram = new SecondLevelKey(nodeHilbertCode, matchingPosQgram );
	                  
	                  // read from the memory
	                  idSet = memHilbPosQgramInvertedMap.get( hilbertPosQgram );
	                  
	                  // read it from the database
	                  if ( idSet == null )
	                  {
	                    idSet = secondLevelInvertedIndex.readObjectList( hilbertPosQgram );              
	                    
	                    if ( idSet == null )
	                    {
	                      memHilbPosQgramInvertedMap.put( hilbertPosQgram, new TreeSet<Integer>() );
	                    }
	                    else
	                    {
	                      memHilbPosQgramInvertedMap.put( hilbertPosQgram, idSet );
	                    }
	                    
	                  }
	                                  
	                  if ( idSet != null )
	                  {                  
	                    // do the prefix filter, the prefix filter ensures that objects must have one matching positional q-gram
	                    approximatePrefixCandidatesInNode.addAll( idSet );  
	                  }
	                }                   
	              }
	            }
	          }
	        }
	        
	        // we are only interested in the objects in the searching node
	        // prune the non-intersecting objects
	        approximatePrefixCandidatesInNode.retainAll( nodeStats._intersectingObjectSet );              
	     
	        // if the candidate size is large, do extra object filter...
	        if ( approximatePrefixCandidatesInNode.size() > sparseThreshold )
	        {              
	          // if the text is short, apply the count filter for positional q-gram
	          if ( query._minPosQgramMatch >= 1 )
	          {     
	            // if the length of the query text is short enough
	            if ( query._queryText.length() - query._smallQValue + 1  <= lamdaValue - query._tau )
	            {       
	              applyCountFilterForPositioinalQgram = true;
	              HashSet<Integer> approximatePrefixObjectForCountFilter = new HashSet<Integer> ();
	              
	              // get the count filter
	              this.approximatePrefixPositionalQgramCountFilterInNode( 
	                  query, query._minPosQgramMatch, nodeHilbertCode, 
	                  approximatePrefixObjectForCountFilter, 
	                  memInfreqPosQgramInvertedMap, memHilbPosQgramInvertedMap );  
	              
	              // retain the result with the approximate substring candidates
	              if ( approximatePrefixObjectForCountFilter.isEmpty() )
	              {
	                approximatePrefixCandidatesInNode.clear();
	              }
	              else
	              {
	                approximatePrefixCandidatesInNode.retainAll( approximatePrefixObjectForCountFilter );
	              }               
	            }          
	            
	            // if the candidate size is large, do q-gram count filter
	            if ( approximatePrefixCandidatesInNode.size() > sparseThreshold )
	            {
	              if ( query._minQgramTokenMatch >= 1 )
	              {
	                HashSet< Integer > approximateSubstringCandidatesInNode = new HashSet< Integer > ();
	                this.approximateSubstringQgramCountFilterAtNode( 
	                    query, query._minQgramTokenMatch, nodeHilbertCode, 
	                    memInfreqQgramTokenInvertedMap, memHilbQgramTokenInvertedMap,
	                    mergeSkipOperator, approximateSubstringCandidatesInNode, approximateSubstringCandidateMap ); 
	                
	                // retain the result with the approximate substring candidates
	                if ( approximateSubstringCandidatesInNode.isEmpty() )
	                {
	                  approximatePrefixCandidatesInNode.clear();
	                }
	                else
	                {
	                  approximatePrefixCandidatesInNode.retainAll( approximateSubstringCandidatesInNode );
	                }   
	              }         
	            }
	            
	            // else apply the count filter for q-gram token
//	            else if ( query._minQgramTokenMatch >= 1 )
//	            {
//	              HashSet< Integer > approximateSubstringCandidatesInNode = new HashSet< Integer > ();
//	              this.approximateSubstringQgramCountFilterAtNode( 
//	                  query, query._minQgramTokenMatch, nodeHilbertCode, 
//	                  memInfreqQgramTokenInvertedMap, memHilbQgramTokenInvertedMap,
//	                  mergeSkipOperator, approximateSubstringCandidatesInNode, approximateSubstringCandidateMap ); 
//	              
//	              // retain the result with the approximate substring candidates
//	              if ( approximateSubstringCandidatesInNode.isEmpty() )
//	              {
//	                approximatePrefixCandidatesInNode.clear();
//	              }
//	              else
//	              {
//	                approximatePrefixCandidatesInNode.retainAll( approximateSubstringCandidatesInNode );
//	              }   
//	            }                   
	          }     
	        }    
	      }
	      
	      // else the query text is very long, the prefix filter can not be applied, use the approximate substring filter.
	      else
	      {
	        HashSet< Integer > approximateSubstringCandidatesInNode = new HashSet< Integer > ();
	        this.approximateSubstringQgramCountFilterAtNode( 
	            query, query._minQgramTokenMatch, nodeHilbertCode, 
	            memInfreqQgramTokenInvertedMap, memHilbQgramTokenInvertedMap,
	            mergeSkipOperator, approximateSubstringCandidatesInNode, approximateSubstringCandidateMap ); 
	        
	        // retain the result with the approximate substring candidates
	        if ( ! approximateSubstringCandidatesInNode.isEmpty() )
	        {
	          // add the approximate substring candidate
	          approximatePrefixCandidatesInNode.addAll( approximateSubstringCandidatesInNode );
	          
	          // prune the non-intersecting objects
	          approximatePrefixCandidatesInNode.retainAll( nodeStats._intersectingObjectSet );      
	        } 
	        
	      }
	      
	      // verify each object candidate                
	      if ( ! approximatePrefixCandidatesInNode.isEmpty() )
	      {
	        for ( int objectid : approximatePrefixCandidatesInNode )
	        {
	          if ( ! approximatePrefixResult.contains( objectid ) )
	          {
	            SpatialObject object = memObjectMap.get( objectid );
	            if ( object == null )
	            {
	              object = objectDatabase.getSpatialObject( objectid );
	              if ( object != null )
	              {
	                memObjectMap.put( objectid, object );
	              }
	            }
	                  
	            // if do the count filer for positional q-gram before
	            if ( applyCountFilterForPositioinalQgram )
	            {
	              // if pass the string verification
	              if ( StringVerification.isTauPrefix( query._queryText, object._text, query._tau ) )
	              {
	                approximatePrefixResult.add( objectid );
	              }
	            }            
	            // else if it passes the q-gram
	            else if ( qgramFilter.approximatePrefixFilter( query._queryText,
	                query._largeQgramTokenPositionMap, query._largeQgramPrefixFilterTokenPositionMap, object._text, query._tau ))
	            {
	              // if pass the string verification
	              if ( StringVerification.isTauPrefix( query._queryText, object._text, query._tau ) )
	              {
	                approximatePrefixResult.add( objectid );
	              }
	            }
	          }                             
	        }
	      }         
	    }
	  }    
  
  
  protected void readIntersectingNodeStatsOfRelaxedRegion( 
     SpatialQuery query, 
     BooleanObject hasRetrievedIntersectingNodesOfRelaxedRegion,
     IntersectingNodeStatsMap intersectingNodeStatsMapOfRelaxedRegion)
  {
    if ( hasRetrievedIntersectingNodesOfRelaxedRegion.isTrue() )
    {
      return;
    }    
    hasRetrievedIntersectingNodesOfRelaxedRegion.setTrue();
    
    Region relaxedRegion = query._queryRegion.getNewEnlargedRegion( this.areaEnlargedRatio );
    quadtree.getIntersectingNodeStatistic( relaxedRegion, intersectingNodeStatsMapOfRelaxedRegion, sparseThreshold );
    
  }
  
  /**
   * add the entries of map2 to map1
   * @param map1
   * @param map2
   */
  private void insertAllEntry ( 
      TreeMap< String, ArrayList<Integer> > map1,
      TreeMap< String, ArrayList<Integer> > map2)
  {
    Iterator<Entry<String, ArrayList<Integer>> > itr2 = map2.entrySet().iterator();
    while ( itr2.hasNext() )
    {
      Entry<String, ArrayList<Integer>> entry = itr2.next();
      map1.put( entry.getKey(), entry.getValue() );
    }   
  }
  
  
  public void printResult( SpatialQuery query, Collection<Integer> collection )
  {
    System.out.println( "this query ends at " + query._queryType );
    System.out.println( "result size: " + collection.size() );      
    
    //System.out.println( collection );
    
    for ( int id : collection )
    {
      System.out.println(  "object " + id + ":\t" + objectDatabase.getSpatialObject( id ).getText() );
    }
    
    System.out.println( );
  }

}
