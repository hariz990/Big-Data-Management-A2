package sortmergejoin;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;


public class MergeSkip
{
  
 
  public void idJoin ( 
     ArrayList< NavigableSet< Integer > > sortedArrayList,
     ArrayList< Integer > weightList,
     int threshold ,
     Collection< Integer > candidates)
  {    
    
    int weightUpperBound = 0;
    for ( int weight : weightList )
    {
      weightUpperBound += weight;
    }
    
    if ( weightUpperBound < threshold )
    {
      return;
    }    
    else if ( weightList.size() == 1 )
    {
      candidates.addAll( sortedArrayList.get( 0 ) );
      return;
    }
    
    
    do
    {
      ArrayList< Integer > minArray = new ArrayList< Integer >();
      TreeSet< Integer > minHeap = new TreeSet< Integer >(); 
      
      // add the min result to the heap
      for ( NavigableSet< Integer > sortedArray : sortedArrayList )
      {    
        int min;
        if ( sortedArray.isEmpty() )
        {
          min = Integer.MAX_VALUE;
        }
        else
        {
          min = sortedArray.first();
        }        
        minArray.add( min );
        minHeap.add( min );
      }        
      
      Iterator< Integer > minHeapItr = minHeap.iterator();        
      
      int accumulativeCount = 0;
      int min = Integer.MAX_VALUE;
      
      boolean isFirst = true;
      boolean firstMatch = false;
      int firstCount = 0;
      
      while ( minHeapItr.hasNext() )
      {
        min = minHeapItr.next();
        
        // the minimum value;
        if ( isFirst )
        {
          isFirst = false;
          for( int i = 0; i < minArray.size(); i++ )
          {
            if ( minArray.get( i ) == min )
            {
              int weight = weightList.get( i );
              firstCount += weight;
              accumulativeCount += weight;
            }        
          }
          
          if ( firstCount >= threshold )
          {
            firstMatch = true;
            candidates.add( min ); 
            break;
          }
        }
        
        // if the minimum value does not match
        else
        {
          for( int i = 0; i < minArray.size(); i++ )
          {
            if ( minArray.get( i ) == min )
            {
              int weight = weightList.get( i );          
              accumulativeCount += weight;
            }        
          }
                  
          if ( accumulativeCount >= threshold )
          {
            break;
          }
        }    
      }
      
      if ( firstMatch )
      {
        // pop the min value for each array
        for( int i = 0; i < minArray.size(); i++ )
        {
          if ( minArray.get( i ) == min )
          {
            NavigableSet< Integer > sortedArray = sortedArrayList.get( i );
            sortedArrayList.set( i, sortedArray.tailSet( min, false ) );
          }        
        }               
      }
      else
      {
        // skip those values which are smaller than the min value        
        if ( min == Integer.MAX_VALUE )
        {
          return;
        }
        
        for( int i = 0; i < minArray.size(); i++ )
        {
          if ( minArray.get( i ) < min )
          {                                                
            NavigableSet< Integer > sortedArray = sortedArrayList.get( i );
            sortedArrayList.set( i, sortedArray.tailSet( min, true ) );
          }        
        }
      }
      
    } while ( ! isAllEmpty( sortedArrayList ) );
    
  }
  
  
  public void idJoin( 
      ArrayList< NavigableSet< Integer > > sortedArrayList,
      ArrayList< Integer > weightList, 
      int threshold, 
      Map<Integer, Integer> candidates )
  {

    int weightUpperBound = 0;
    for ( int weight : weightList )
    {
      weightUpperBound += weight;
    }

    // the maximum weight is smaller than the threshold
    if ( weightUpperBound < threshold )
    {
      return;
    }
    // if the maximum weight is greater than the threshold and there is only one list, 
    // directly output the list
    else if ( weightList.size() == 1 )
    {
      
      NavigableSet< Integer > idSet = sortedArrayList.get( 0 );
      int weight = weightList.get( 0 );
      
      // add all the ids to the candidate      
      if ( idSet != null )
      {
        for ( int id : idSet )
        {
          candidates.put( id, weight );
        }
      }
      return;
    }

    do
    {
      ArrayList< Integer > minArray = new ArrayList< Integer >();
      TreeSet< Integer > minHeap = new TreeSet< Integer >();

      // add the min result to the heap
      for ( NavigableSet< Integer > sortedArray : sortedArrayList )
      {
        int min;
        if ( sortedArray.isEmpty() )
        {
          min = Integer.MAX_VALUE;
        }
        else
        {
          min = sortedArray.first();
        }
        minArray.add( min );
        minHeap.add( min );
      }

      Iterator< Integer > minHeapItr = minHeap.iterator();

      int accumulativeCount = 0;
      int min = Integer.MAX_VALUE;

      boolean isFirst = true;
      boolean firstMatch = false;
      int firstCount = 0;

      while ( minHeapItr.hasNext() )
      {
        min = minHeapItr.next();

        // the minimum value;
        if ( isFirst )
        {
          isFirst = false;
          for ( int i = 0; i < minArray.size(); i++ )
          {
            if ( minArray.get( i ) == min )
            {
              int weight = weightList.get( i );
              firstCount += weight;
              accumulativeCount += weight;
            }
          }

          if ( firstCount >= threshold )
          {
            firstMatch = true;
            candidates.put( min, firstCount );
            break;
          }
        }

        // if the minimum value does not match
        else
        {
          for ( int i = 0; i < minArray.size(); i++ )
          {
            if ( minArray.get( i ) == min )
            {
              int weight = weightList.get( i );
              accumulativeCount += weight;
            }
          }

          if ( accumulativeCount >= threshold )
          {
            break;
          }
        }
      }

      if ( firstMatch )
      {
        // pop the min value for each array
        for ( int i = 0; i < minArray.size(); i++ )
        {
          if ( minArray.get( i ) == min )
          {
            // sortedArrayList.get( i ).pollFirst();
            NavigableSet< Integer > sortedArray = sortedArrayList.get( i );
            sortedArrayList.set( i, sortedArray.tailSet( min, false ) );
          }
        }
      }
      else
      {
        // skip those values which are smaller than the min value
        
        if ( min == Integer.MAX_VALUE )
        {
          return;
        }
        
        for ( int i = 0; i < minArray.size(); i++ )
        {
          if ( minArray.get( i ) < min )
          {
            NavigableSet< Integer > sortedArray = sortedArrayList.get( i );
            sortedArrayList.set( i, sortedArray.tailSet( min, true ) );
          }
        }
      }

    }
    while ( !isAllEmpty( sortedArrayList ) );

  }
  

  
  public void nodeJoin( 
      ArrayList< NavigableMap< String, Integer >> sortedMapList,
      ArrayList< Integer > weightList, 
      int threshold, 
      Collection< String > nodeCandidates )
  {
    
    int weightUpperBound = 0;
    for ( int weight : weightList )
    {
      weightUpperBound += weight;
    }
    
    if ( weightUpperBound < threshold )
    {
      return;
    }    
    else if ( weightList.size() == 1 )
    {
      nodeCandidates.addAll( sortedMapList.get( 0 ).keySet() );
      return;
    }
    
    
    do
    {
      ArrayList< String > minArray = new ArrayList< String >();
      TreeSet< String > minHeap = new TreeSet< String >();

      // add the min result to the heap
      for ( NavigableMap< String, Integer > sortedMap : sortedMapList )
      {
        String min;
        if ( sortedMap.isEmpty() )
        {
          min = "4";
        }
        else
        {
          min = sortedMap.firstKey();
        }
        minArray.add( min );
        minHeap.add( min );
      }

      Iterator< String > minHeapItr = minHeap.iterator();

      int accumulativeCount = 0;
      String min = "4";

      boolean isFirst = true;
      boolean firstMatch = false;
      int firstCount = 0;

      while ( minHeapItr.hasNext() )
      {
        min = minHeapItr.next();

        // the minimum value;
        if ( isFirst )
        {
          isFirst = false;
          for ( int i = 0; i < minArray.size(); i++ )
          {
            if ( minArray.get( i ).equals( min ) )
            {
              int weight = weightList.get( i );
              firstCount += weight;
              accumulativeCount += weight;
            }
          }

          if ( firstCount >= threshold )
          {
            firstMatch = true;
            nodeCandidates.add( min );
            break;
          }
        }

        // if the minimum value does not match
        else
        {
          for ( int i = 0; i < minArray.size(); i++ )
          {
            if ( minArray.get( i ).equals( min ) )
            {
              int weight = weightList.get( i );
              accumulativeCount += weight;
            }
          }

          if ( accumulativeCount >= threshold )
          {
            break;
          }
        }
      }

      if ( firstMatch )
      {
        // pop the min value for each array
        for ( int i = 0; i < minArray.size(); i++ )
        {
          if ( minArray.get( i ).equals( min ) )
          {
            NavigableMap< String, Integer > sortedMap = sortedMapList.get( i );
            sortedMapList.set( i, sortedMap.tailMap( min, false ) );
          }
        }
      }
      else
      {
        // skip those values which are smaller than the min value
        if ( min.equals( "4" ) )
        {
          return;
        }
        
        for ( int i = 0; i < minArray.size(); i++ )
        {
          if ( minArray.get( i ).compareTo( min ) < 0 )
          {
            NavigableMap< String, Integer > sortedMap = sortedMapList.get( i );
            sortedMapList.set( i, sortedMap.tailMap( min, true ) );
          }
        }
      }
    }
    while ( !isAllMapEmpty( sortedMapList ) );

  }
  
  /**
   * 
   * @param sortedMapList the inverted list for q-gram -> (node id, count) List 
   * @param weightList
   * @param threshold
   * @param nodeCandidates
   */
  public void nodeJoin( 
      ArrayList< NavigableMap< String, Integer >> sortedMapList, 
      ArrayList< Integer > weightList, 
      int threshold, 
      Map< String, Integer > nodeCandidates )
  {

    int weightUpperBound = 0;
    for ( int weight : weightList )
    {
      weightUpperBound += weight;
    }

    if ( weightUpperBound < threshold )
    {
      return;
    }
    else if ( weightList.size() == 1 )
    {
      
      Set<String> nodeIdSet =  sortedMapList.get( 0 ).keySet();
      int weight = weightList.get( 0 );
      
      if ( nodeIdSet != null )
      {
        for ( String nodeId : nodeIdSet )
        {
          nodeCandidates.put( nodeId, weight );
        }
      }     
      return;
    }


    do
    {
      ArrayList< String > minArray = new ArrayList< String >();
      TreeSet< String > minHeap = new TreeSet< String >();

      // add the min result to the heap
      for ( NavigableMap< String, Integer > sortedMap : sortedMapList )
      {
        String min;
        if ( sortedMap.isEmpty() )
        {
          min = "4";
        }
        else
        {
          min = sortedMap.firstKey();
        }
        minArray.add( min );
        minHeap.add( min );
      }

      Iterator< String > minHeapItr = minHeap.iterator();

      int accumulativeCount = 0;
      String min = "4";

      boolean isFirst = true;
      boolean firstMatch = false;
      int firstCount = 0;

      while ( minHeapItr.hasNext() )
      {
        min = minHeapItr.next();

        // the minimum value;
        if ( isFirst )
        {
          isFirst = false;
          for ( int i = 0; i < minArray.size(); i++ )
          {
            if ( minArray.get( i ).equals( min ) )
            {
              int weight = weightList.get( i );
              firstCount += weight;
              accumulativeCount += weight;
            }
          }

          if ( firstCount >= threshold )
          {
            firstMatch = true;           
            nodeCandidates.put( min, firstCount );
            
            break;
          }
        }

        // if the minimum value does not match
        else
        {
          for ( int i = 0; i < minArray.size(); i++ )
          {
            if ( minArray.get( i ).equals( min ) )
            {
              int weight = weightList.get( i );
              accumulativeCount += weight;
            }
          }

          if ( accumulativeCount >= threshold )
          {
            break;
          }
        }
      }

      if ( firstMatch )
      {
        // pop the min value for each array
        for ( int i = 0; i < minArray.size(); i++ )
        {
          if ( minArray.get( i ).equals( min ) )
          {
            NavigableMap< String, Integer > sortedMap = sortedMapList.get( i );
            sortedMapList.set( i, sortedMap.tailMap( min, false ) );
          }
        }
      }
      else
      {
        // skip those values which are smaller than the min value
        if ( min.equals( "4" ) )
        {
          return;
        }
        
        for ( int i = 0; i < minArray.size(); i++ )
        {
          if ( minArray.get( i ).compareTo( min ) < 0 )
          {
            NavigableMap< String, Integer > sortedMap = sortedMapList.get( i );
            sortedMapList.set( i, sortedMap.tailMap( min, true ) );
          }
        }
      }
    }
    while ( !isAllMapEmpty( sortedMapList ) );

  }

  
  
  public boolean isAllEmpty( final ArrayList< NavigableSet< Integer > > sortedArrayList )
  {    
    for ( NavigableSet< Integer > array : sortedArrayList )
    {
      if ( ! array.isEmpty() )
      {
        return false;
      }
    }
    return true;        
  }
  
  
  public boolean isAllMapEmpty( final ArrayList< NavigableMap< String, Integer > > sortedArrayList )
  {    
    for ( NavigableMap< String, Integer > array : sortedArrayList )
    {
      if ( ! array.isEmpty() )
      {
        return false;
      }
    }
    return true;        
  }
  
  
  
  public static void main( String[] args )
  {
    TreeSet< Integer > set1 = new TreeSet< Integer >();
    TreeSet< Integer > set2 = new TreeSet< Integer >();
    TreeSet< Integer > set3 = new TreeSet< Integer >();
    TreeSet< Integer > set4 = new TreeSet< Integer >();
    TreeSet< Integer > set5 = new TreeSet< Integer >();
    
    
    set1.add( 1 );
    set1.add( 2 );
    set1.add( 3 );
    set1.add( 4 );
    set1.add( 5 );   

    
    set2.add( 3 );
    set2.add( 4 );
    set2.add( 5 );
    set2.add( 6 );
    set2.add( 7 );
    
    
    set3.add( 3 );
    set3.add( 4 );
    set3.add( 5 );
    set3.add( 8 );
    set3.add( 9 );
    
    
    set4.add( 6 );
    set4.add( 7 );
    set4.add( 8 );
    set4.add( 9 );        
    
    set5.add( 9 );
    set5.add( 10 );
    
    
    
    ArrayList< NavigableSet< Integer >> arrayList = new ArrayList< NavigableSet< Integer >>();    
    arrayList.add( set1 );
    arrayList.add( set2 );
    arrayList.add( set3 );
    arrayList.add( set4 );
    arrayList.add( set5 );
    
    ArrayList< Integer > weightList = new ArrayList< Integer >(); 
    
    weightList.add( 1 );
    weightList.add( 1 );
    weightList.add( 1 );
    weightList.add( 1 );
    weightList.add( 1 );
    
    
    int threshold = 3;
    ArrayList< Integer > candidates = new ArrayList< Integer >();
    
    
    MergeSkip operator = new MergeSkip();    
    operator.idJoin( arrayList, weightList, threshold, candidates );
    
    System.out.println( candidates );
    
  }
  
  
  
}
