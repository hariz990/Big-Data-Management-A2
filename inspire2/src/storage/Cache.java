package storage;

import java.util.HashMap;
import java.util.TreeSet;

import unit.HilbertCountMap;
import unit.IntersectingNodeStatsMap;
import unit.PositionalQgram;
import unit.SecondLevelKey;
import unit.SpatialObject;


public class Cache
{
  
  // intersectingNodeStatsMap
  IntersectingNodeStatsMap intersectingNodeStatsMap;
 
  IntersectingNodeStatsMap intersectingNodeStatsMapOfRelaxedRegion;

  // spatial objects that are visited before
  HashMap< Integer, SpatialObject > memObjectMap;
   
  // memory infrequent positional inverted map
  HashMap< PositionalQgram, TreeSet< Integer >> memInfreqPosQgramInvertedMap;
      
  // memory infrequent positional inverted map
  HashMap< String, TreeSet< Integer >> memInfreqQgramTokenInvertedMap;
       
  // memPosQgramCountPairInvertedMap
  HashMap< PositionalQgram, HilbertCountMap > memPosQgramCountPairInvertedMap;
      
  // memQgramTokenCountPairInvertedMap
  HashMap< String, HilbertCountMap > memQgramTokenCountPairInvertedMap;
      
  // memHilbPosQgramInvertedMap
  HashMap< SecondLevelKey, TreeSet< Integer >> memHilbPosQgramInvertedMap;
      
  // memHilbPosQgramInvertedMap
  HashMap< String, TreeSet< Integer >> memHilbQgramTokenInvertedMap;
          
  public Cache()
  {
    // intersectingNodeStatsMap
    intersectingNodeStatsMap = new IntersectingNodeStatsMap();
   
    intersectingNodeStatsMapOfRelaxedRegion = new IntersectingNodeStatsMap();

    // spatial objects that are visited before
    memObjectMap = new HashMap< Integer, SpatialObject >();
        
    // memory infrequent positional inverted map
    memInfreqPosQgramInvertedMap = new HashMap< PositionalQgram, TreeSet< Integer >>();

    // memory infrequent positional inverted map
    memInfreqQgramTokenInvertedMap = new HashMap< String, TreeSet< Integer >>();    

    // memPosQgramCountPairInvertedMap
    memPosQgramCountPairInvertedMap = new HashMap< PositionalQgram, HilbertCountMap >();

    // memQgramTokenCountPairInvertedMap
    memQgramTokenCountPairInvertedMap = new HashMap< String, HilbertCountMap >();

    // memHilbPosQgramInvertedMap
    memHilbPosQgramInvertedMap = new HashMap< SecondLevelKey, TreeSet< Integer >>();

    // memHilbPosQgramInvertedMap
    memHilbQgramTokenInvertedMap = new HashMap< String, TreeSet< Integer >>();
    
  }

}
