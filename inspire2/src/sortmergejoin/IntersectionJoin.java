package sortmergejoin;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.TreeSet;


public class IntersectionJoin <X, Y>
{

  /**
   * the computational cost is O(m+n), which is the summation of the sizes of two maps, 
   * the result is directly reflected in the first parameter
   * @param treemap1 
   * @param treemap2
   */
  public void join( 
    TreeMap< String, X > treemap1,
    final TreeMap< String, Y > treemap2 )
  {
    if ( treemap1.isEmpty() )
    {
      return;
    }
    
    Iterator< Entry< String, X >> mapItr = treemap1.entrySet().iterator();
    Iterator< Entry< String, Y >> intersItr = treemap2.entrySet().iterator();

    int keyComparedValue = 0;
    String mapKey = null;
    String IntersKey = null;

    while ( true )
    {
      // if it is not the first entry, compute the key compared value
      if ( mapKey != null )
      {
        keyComparedValue = mapKey.compareTo( IntersKey );
      }

      // key match
      if ( keyComparedValue == 0 )
      {
        if ( mapItr.hasNext() && intersItr.hasNext() )
        {
          mapKey = mapItr.next().getKey();
          IntersKey = intersItr.next().getKey();
        }
        else
        {
          while ( mapItr.hasNext() )
          {
            mapItr.next();
            mapItr.remove();
          }                              
          break;
        }
      }
      // the map key is smaller
      else if ( keyComparedValue < 0 )
      {
        // remove the entry
        mapItr.remove();
        if ( mapItr.hasNext() )
        {
          mapKey = mapItr.next().getKey();
        }
        else
        {
          break;
        }
      }
      // the intersecting key is smaller
      else
      {
        if ( intersItr.hasNext() )
        {
          IntersKey = intersItr.next().getKey();
        }
        else
        {
          mapItr.remove();
          while ( mapItr.hasNext() )
          {
            mapItr.next();
            mapItr.remove();
          }
          break;
        }
      }
    }
  }
  
  
  /*
  public void join( TreeSet< String > treeSet, final TreeMap< String, Y > treemap )
  {
//    TreeSet< String > tempSet = (TreeSet< String >) treeSet.clone();
    
    
    Iterator< String > mapItr = treeSet.iterator();
    Iterator< Entry< String, Y >> intersItr = treemap.entrySet().iterator();

    int keyComparedValue = 0;
    String mapKey = null;
    String IntersKey = null;

    while ( true )
    {
      // if it is not the first entry, compute the key compared value
      if ( mapKey != null )
      {
        keyComparedValue = mapKey.compareTo( IntersKey );
      }

      // key match
      if ( keyComparedValue == 0 )
      {
        if ( mapItr.hasNext() && intersItr.hasNext() )
        {
          mapKey = mapItr.next();
          IntersKey = intersItr.next().getKey();
        }
        else
        {
          while ( mapItr.hasNext() )
          {
            mapItr.next();
            mapItr.remove();
          } 
          break;
        }
      }
      // the map key is smaller
      else if ( keyComparedValue < 0 )
      {
        // remove the entry
        mapItr.remove();
        if ( mapItr.hasNext() )
        {
          mapKey = mapItr.next();
        }
        else
        {
          break;
        }
      }
      // the intersecting key is smaller
      else
      {
        if ( intersItr.hasNext() )
        {
          IntersKey = intersItr.next().getKey();
        }
        else
        {
          mapItr.remove();
          while ( mapItr.hasNext() )
          {
            mapItr.next();
            mapItr.remove();
          }                   
          break;
        }
      }
    }
  }
  */
  
  
  
  public void join( TreeSet< String > set1, final TreeSet< String > set2 )
  {
    if ( set1.isEmpty() )
    {
      return;
    }
    
    Iterator< String > mapItr = set1.iterator();
    Iterator< String > intersItr = set2.iterator();

    int keyComparedValue = 0;
    String mapKey = null;
    String IntersKey = null;

    while ( true )
    {
      // if it is not the first entry, compute the key compared value
      if ( mapKey != null )
      {
        keyComparedValue = mapKey.compareTo( IntersKey );
      }

      // key match
      if ( keyComparedValue == 0 )
      {
        if ( mapItr.hasNext() && intersItr.hasNext() )
        {
          mapKey = mapItr.next();
          IntersKey = intersItr.next();
        }
        else
        {
          while ( mapItr.hasNext() )
          {
            mapItr.next();
            mapItr.remove();
          } 
          break;
        }
      }
      // the map key is smaller
      else if ( keyComparedValue < 0 )
      {
        // remove the entry
        mapItr.remove();
        if ( mapItr.hasNext() )
        {
          mapKey = mapItr.next();
        }
        else
        {
          break;
        }
      }
      // the intersecting key is smaller
      else
      {
        if ( intersItr.hasNext() )
        {
          IntersKey = intersItr.next();
        }
        else
        {
          mapItr.remove();
          while ( mapItr.hasNext() )
          { 
            mapItr.next();
            mapItr.remove();
          }
          break;
        }
      }
    }
  }
  
  
  
//  public void join ( HilbertCountMap hilbertCountMap, IntersectingNodeStatsMap intersectingNodeStatistic )
//  {
//    Iterator< Entry< String, Integer > > mapItr = hilbertCountMap._map.entrySet().iterator();    
//    Iterator< Entry< String, NodeStatistic > > intersItr = intersectingNodeStatistic.entrySet().iterator();    
//    
//    int keyComparedValue = 0;
//    String mapKey = null;
//    String IntersKey = null;    
//    
//    while ( true )
//    {
//      // if it is not the first entry, compute the key compared value
//      if ( mapKey != null )
//      {
//        keyComparedValue = mapKey.compareTo( IntersKey );
//      }
//      
//      // key match
//      if ( keyComparedValue == 0 )
//      {
//        if ( mapItr.hasNext() && intersItr.hasNext() )
//        {          
//          mapKey = mapItr.next().getKey();
//          IntersKey = intersItr.next().getKey();          
//        }   
//        else
//        {
//          break;
//        }
//      }      
//      // the map key is smaller
//      else if ( keyComparedValue < 0 )
//      {
//        // remove the entry
//        mapItr.remove();
//        if ( mapItr.hasNext() )
//        {          
//          mapKey = mapItr.next().getKey();
//        }
//        else
//        {
//          break;
//        }
//      }
//      // the intersecting key is smaller
//      else
//      {
//        if ( intersItr.hasNext() ) 
//        {
//          IntersKey = intersItr.next().getKey();  
//        }
//        else
//        {
//          break;
//        }
//      }
//    }         
//  }
  
  
  
  
  
}
