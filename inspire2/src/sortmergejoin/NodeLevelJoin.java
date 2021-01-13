package sortmergejoin;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.TreeMap;

import unit.HilbertCountMap;
import unit.comparator.HilbertComparator;

public class NodeLevelJoin
{

 
  public HilbertComparator _hilbertComparator;
  
  public NodeLevelJoin()
  {
    _hilbertComparator = new HilbertComparator();
  }
  
  
  public TreeMap< String, ArrayList< Integer >> join( ArrayList< HilbertCountMap > vector )
  {
    if ( vector.size() == 1 )
    {
      TreeMap< String, ArrayList< Integer >> resultMap = new TreeMap< String, ArrayList< Integer >>();
      
      HilbertCountMap map = vector.get( 0 );
      Iterator< Entry< String, Integer > > itr = map._map.entrySet().iterator();
      while( itr.hasNext() )
      {
        Entry< String, Integer > entry = itr.next();
        String hilbertCode = entry.getKey();        
        ArrayList< Integer > list = new ArrayList< Integer >();
        list.add( entry.getValue() );
        resultMap.put( hilbertCode, list );
      }
      return resultMap;      
    }
    
    
    HilbertCountMap firstMap = vector.get( 0 );
    String maxOfMinKey = firstMap._map.firstKey();
    String minOfMaxKey = firstMap._map.lastKey();

    for ( int i = 1; i < vector.size(); i++ )
    {
      TreeMap< String, Integer > currentMap = vector.get( i )._map;
      String currentMinKey = currentMap.firstKey();
      String currentMaxKey = currentMap.lastKey();

//      if ( maxOfMinKey.compareTo( currentMinKey ) < 0 )
      if ( _hilbertComparator.compare( maxOfMinKey, currentMinKey ) < 0 )
      {
        maxOfMinKey = currentMinKey;
      }

//      if ( minOfMaxKey.compareTo( currentMaxKey ) > 0 )
      if ( _hilbertComparator.compare( minOfMaxKey, currentMaxKey ) > 0 )
      {
        minOfMaxKey = currentMaxKey;
      }
    }
//    if ( maxOfMinKey.compareTo( minOfMaxKey ) > 0 ) 
    if ( _hilbertComparator.compare( maxOfMinKey, minOfMaxKey ) > 0 )
      return null;

    ArrayList< NavigableMap< String, Integer >> tempVector =
        new ArrayList< NavigableMap< String, Integer >>();

    for ( HilbertCountMap map : vector )
    {
      NavigableMap< String, Integer > subMap = map._map.subMap( maxOfMinKey, true, minOfMaxKey, true );             
//      NavigableMap< String, Integer > tempMap = new TreeMap< String, Integer > ();
//      
//      Iterator<Entry<String, Integer> > itr = subMap.entrySet().iterator();
//      while( itr.hasNext() )
//      {
//        Entry<String, Integer> entry = itr.next();
//        tempMap.put( entry.getKey(), entry.getValue() );
//      }            
//      tempVector.add( tempMap );
      
      tempVector.add ( subMap );
    }

    return mergeJion( tempVector );
  }
  
  
//  public TreeMap< String, ArrayList< Integer >> join( ArrayList< HilbertCountMap > vector,
//      TreeMap< String, NodeStatistic > intersectingMap )
//  {
//    // only keep the nodes that intersect with the query region
//    Set< String > intersetingNodes = intersectingMap.keySet();
//    for ( int i = 0; i < vector.size(); i++ )
//    {
//      vector.get( i )._map.keySet().retainAll( intersetingNodes );
//    }
//
//    return join( vector );
//  }
//
//  
//  public TreeSet< String > hilbertCodeJoin( ArrayList< TreeSet< String > > list )
//  {
//    TreeSet< String > firstSet = list.get( 0 );
//    String maxOfMinKey = firstSet.first();
//    String minOfMaxKey = firstSet.last();
//
//    for ( int i = 1; i < list.size(); i++ )
//    {
//      TreeSet< String > currentSet = list.get( i );
//      String currentMinKey = currentSet.first();
//      String currentMaxKey = currentSet.last();
//
////      if ( maxOfMinKey.compareTo( currentMinKey ) < 0 )
//      if ( _hilbertComparator.compare( maxOfMinKey, currentMinKey ) < 0 )
//      {
//        maxOfMinKey = currentMinKey;
//      }
//
////      if ( minOfMaxKey.compareTo( currentMaxKey ) > 0 )
//      if ( _hilbertComparator.compare( minOfMaxKey, currentMaxKey ) > 0 )
//      {
//        minOfMaxKey = currentMaxKey;
//      }
//    }
////    if ( maxOfMinKey.compareTo( minOfMaxKey ) > 0 ) 
//    if ( _hilbertComparator.compare( maxOfMinKey, minOfMaxKey ) > 0 )
//      return null;
//
//    ArrayList< TreeSet< String >> tempList = new ArrayList< TreeSet< String >>();
//
//    for ( TreeSet< String > set : list )
//    {
//      tempList.add( (TreeSet< String >) set.subSet( maxOfMinKey, true, minOfMaxKey, true ) );
//    }
//
//    return hilbertCodeMergeJion( tempList );
//  }




  private TreeMap< String, ArrayList< Integer >> mergeJion(
      ArrayList< NavigableMap< String, Integer >> vector )
  {
    TreeMap< String, ArrayList< Integer >> result = new TreeMap< String, ArrayList< Integer >>( new HilbertComparator() );

    while ( !isOneEmpty( vector ) )
    {
      String minOfMinKey = vector.get( 0 ).firstEntry().getKey();
      String maxOfMinKey = minOfMinKey;

      for ( int i = 1; i < vector.size(); i++ )
      {

        String currentMinKey = vector.get( i ).firstEntry().getKey();

//        if ( minOfMinKey.compareTo( currentMinKey ) > 0 )
        if ( _hilbertComparator.compare( minOfMinKey, currentMinKey ) > 0 )
        {
          minOfMinKey = currentMinKey;
        }
//        else if ( maxOfMinKey.compareTo( currentMinKey ) < 0 )
        else if ( _hilbertComparator.compare( maxOfMinKey, currentMinKey ) < 0 )
        {
          maxOfMinKey = currentMinKey;
        }
      }

      // same number, add to result
      if ( minOfMinKey.equals( maxOfMinKey ) )
      {
        ArrayList< Integer > countVector = new ArrayList< Integer >();
        for ( int i = 0; i < vector.size(); i++ )
        {
//          countVector.add( vector.get( i ).pollFirstEntry().getValue() );
          
          NavigableMap< String, Integer > currentMap = vector.get( i );
          countVector.add( currentMap.firstEntry().getValue() );
          vector.set( i, currentMap.tailMap( maxOfMinKey, false ) );          
        }
        // minOfMinKey is the hilbert code of a node
        result.put( minOfMinKey, countVector );

      }
      else
      {
        for ( int i = 0; i < vector.size(); i++ )
        {
          NavigableMap< String, Integer > currentMap = vector.get( i );
          if ( currentMap.firstKey().compareTo( maxOfMinKey ) != 0 )
          {
            vector.set( i, currentMap.tailMap( maxOfMinKey, true ) );
          }
        }
      }
    }

    return result.isEmpty() ? null : result;
  }
  
  
  
  
//  private TreeSet< String > hilbertCodeMergeJion(
//    ArrayList< TreeSet< String >> list )
//{
//    TreeSet< String > result =
//      new TreeSet< String >( new HilbertComparator() );
//
//  while ( ! containsEmptyElement( list ) )
//  {
//    String minOfMinKey = list.get( 0 ).first();
//    String maxOfMinKey = minOfMinKey;
//
//    for ( int i = 1; i < list.size(); i++ )
//    {
//      String currentMinKey = list.get( i ).first();
//
////      if ( minOfMinKey.compareTo( currentMinKey ) > 0 )
//      if ( _hilbertComparator.compare( minOfMinKey, currentMinKey ) > 0 )
//      {
//        minOfMinKey = currentMinKey;
//      }
////      else if ( maxOfMinKey.compareTo( currentMinKey ) < 0 )
//      else if ( _hilbertComparator.compare( maxOfMinKey, currentMinKey ) < 0 )
//      {
//        maxOfMinKey = currentMinKey;
//      }
//    }
//
//    // same number, add to result
//    if ( minOfMinKey.equals( maxOfMinKey ) )
//    {      
//      // minOfMinKey is the hilbert code of a node
//      result.add( minOfMinKey );
//      for ( TreeSet< String > currentSet : list )
//      {
//        currentSet.pollFirst();
//      }
//    }
//    else
//    {
//      for ( int i = 0; i < list.size(); i++ )
//      {
//        TreeSet< String > currentSet = list.get( i );
//        if ( currentSet.first().compareTo( maxOfMinKey ) != 0 )
//        {
//          list.set( i, (TreeSet< String >) currentSet.tailSet( maxOfMinKey, true ) );
//        }
//      }
//    }
//  }
//
//  return result.isEmpty() ? null : result;
//}


  private boolean isOneEmpty( final ArrayList< NavigableMap< String, Integer >> vector )
  {
    for ( int i = 0; i < vector.size(); i++ )
    {

      NavigableMap< String, Integer > map = vector.get( i );

      if ( map == null || map.isEmpty() ) return true;
    }
    return false;
  }
  
  
//  private boolean containsEmptyElement ( final ArrayList< TreeSet <String>> list )
//  {
//    for ( int i = 0; i < list.size(); i++ )
//    {
//      TreeSet< String > set = list.get( i );
//      if ( set == null || set.isEmpty() ) 
//        return true;
//    }
//    return false;
//  }

}
