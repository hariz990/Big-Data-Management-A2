package sortmergejoin;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.TreeSet;


public class ContinuousMergeSkip extends MergeSkip
{
    
  
  public int continuousIdJoinAdvanced ( 
      ArrayList< NavigableSet< Integer > > oldSortedArrayList,
      ArrayList< NavigableSet< Integer > > newSortedArrayList, 
      ArrayList< Integer > oldWeightList,
      ArrayList< Integer > newWeightList, 
      int oldThreshold, 
      int newThreshold,
      Collection< Integer > oldCandidates, 
      Collection< Integer > newCandidates )
  {
    int numberOfNewLists = 0;
    if ( newWeightList != null )
    {
      for ( int weight : newWeightList )
      {
        numberOfNewLists += weight;
      }
    }

    // if there is no new list, no need to run
    if ( numberOfNewLists == 0 )
    {
      return -1;
    }

    // handle the removed id in three cases:
    // case 1 : the number of additional matches is greater than the number of new lists
    // only need to handle the ids in the oldCandidates
    if ( newThreshold >= oldThreshold + numberOfNewLists )
    {
      this.verifyCandidatesInAllLists( 
        oldSortedArrayList, newSortedArrayList,
        oldWeightList, newWeightList, 
        oldThreshold, newThreshold,
        oldCandidates, newCandidates );
      
      return 1;
    }
    // case 2: it requires additional matches and the number of matches is smaller than the number
    // of new lists
    // partition the list and merge the partitions
    else if ( newThreshold >= oldThreshold )
    {
      this.continuousIdJoinForCaseTwo(
        oldSortedArrayList, newSortedArrayList,
        oldWeightList, newWeightList, 
        oldThreshold, newThreshold,
        oldCandidates, newCandidates );
      
      return 2;
    }
    // case 3: newThreshold < oldThreshold
    // restart merging lists from sketch
    else
    {
      ArrayList< NavigableSet< Integer >> idList = new ArrayList< NavigableSet< Integer > >();
      ArrayList< Integer > weightList = new ArrayList< Integer >();

      idList.addAll( oldSortedArrayList );
      idList.addAll( newSortedArrayList );
      weightList.addAll( oldWeightList );
      weightList.addAll( newWeightList );

      // merge all the inverted lists
      this.idJoin( idList, weightList, newThreshold, newCandidates );
      
      return 3;
    }
  }
  
  
  public void continuousIdJoinForCaseTwo( 
      ArrayList< NavigableSet< Integer > > oldSortedArrayList,                                   
      ArrayList< NavigableSet< Integer > > newSortedArrayList,
      ArrayList< Integer > oldWeightList,
      ArrayList< Integer > newWeightList, 
      int oldThreshold, 
      int newThreshold,
      Collection< Integer > oldCandidates, 
      Collection< Integer > newCandidates )
  {        
    /*
     * for the ids not in the oldCandidates, use the partition method to solve
     */
    ArrayList< NavigableSet< Integer > > allLists = new ArrayList< NavigableSet< Integer > >();
    allLists.addAll( oldSortedArrayList );
    allLists.addAll( newSortedArrayList );
        
    int newListStartingIndex = oldSortedArrayList.size(); 
    
    ArrayList< Integer > weightList = new ArrayList< Integer >();
    weightList.addAll( oldWeightList );
    weightList.addAll( newWeightList );
    
    
    int weightUpperBound = 0;
    for ( int weight : weightList )
    {
      weightUpperBound += weight;
    }

    // the maximum weight is smaller than the threshold
    if ( weightUpperBound < newThreshold )
    {
      return;
    }
    // if the maximum weight is greater than the threshold and there is only one list, 
    // directly output the list
    else if ( weightList.size() == 1 )
    {        
      NavigableSet< Integer > idSet = allLists.get( 0 );      
 
      if ( idSet != null )
      {
        newCandidates.addAll( idSet );
      }
      return;
    }
            
//     for the ids in the oldCandidates, and delete the oldCandidate as well    
    this.verifyCandidatesInAllLists( allLists, weightList, oldThreshold, newThreshold, oldCandidates, newCandidates );
        
        
    // threshold for allList and newLists
    int thresholdForAllLists = newThreshold;
    int thresholdForNewLists = ( newThreshold - oldThreshold + 1 );    
           
    // start merging the inverted lists
    do
    {      
      ArrayList< Integer > minArray = new ArrayList< Integer >();
     
      // two minHeap, One for allLists, the other for the newLists
      TreeSet< Integer > minHeapForAllLists = new TreeSet< Integer >();
      TreeSet< Integer > minHeapForNewLists = new TreeSet< Integer >();
      
      // add the min result to the heap
      for ( int i = 0; i < allLists.size(); i++ )
      {       
        NavigableSet< Integer > sortedArray = allLists.get( i );
        
        int min;
        if ( sortedArray.isEmpty() )
        {
          min = Integer.MAX_VALUE;
        }
        else
        {
          min = sortedArray.first();
        }
        
        // add it to the min array to record the frontier of each inverted list
        minArray.add( min );
        minHeapForAllLists.add( min );
        
        if ( i >= newListStartingIndex )
        {
          minHeapForNewLists.add( min );
        }
      }                                         
      
      
      // for the new inverted lists, get the (thresholdForNewLists)^th smallest value.
      Iterator< Integer > minHeapNewItr = minHeapForNewLists.iterator();
      int accumulativeCountForNewLists = 0;
      int minValueForNewLists = Integer.MAX_VALUE;

          
      while ( minHeapNewItr.hasNext() )
      {
        minValueForNewLists = minHeapNewItr.next();
        
        for ( int i = newListStartingIndex; i < minArray.size(); i++ )
        {
          if ( minArray.get( i ) == minValueForNewLists )
          {
            int weight = weightList.get( i );
            accumulativeCountForNewLists += weight;
          }
          
          if ( accumulativeCountForNewLists >= thresholdForNewLists )
          {
            break;
          }
        }
        if ( accumulativeCountForNewLists >= thresholdForNewLists )
        {
          break;
        }
      }                       
      
      
    
      // for all inverted lists, get the (thresholdForAllLists)^the smallest value
      Iterator< Integer > minHeapAllItr = minHeapForAllLists.iterator();
      int accumulativeCountForAllLists = 0;
      int minValueForAllLists = Integer.MAX_VALUE;
      boolean isFirstForAllLists = true;
      boolean firstMatchForAllLists = false;     
    
      while ( minHeapAllItr.hasNext() )
      {
        minValueForAllLists = minHeapAllItr.next();

        // the minimum value;
        if ( isFirstForAllLists )
        {
          isFirstForAllLists = false;
          for ( int i = 0; i < minArray.size(); i++ )
          {
            if ( minArray.get( i ) == minValueForAllLists )
            {
              int weight = weightList.get( i );     
              accumulativeCountForAllLists += weight;
            }
            if ( accumulativeCountForAllLists >= thresholdForAllLists )
            {
              firstMatchForAllLists = true;
              break;
            }            
          }
          if ( accumulativeCountForAllLists >= thresholdForAllLists )
          {
            firstMatchForAllLists = true;
            break;
          }
        }

        // if the minimum value does not match
        else
        {
          for ( int i = 0; i < minArray.size(); i++ )
          {
            if ( minArray.get( i ) == minValueForAllLists )
            {
              int weight = weightList.get( i );
              accumulativeCountForAllLists += weight;
              if ( accumulativeCountForAllLists >= thresholdForAllLists )
              {
                break;
              }
            }
          }
          if ( accumulativeCountForAllLists >= thresholdForAllLists )
          {
            break;
          }
        }
      }
                  
      
      if ( ( minValueForNewLists <= minValueForAllLists ) &&  firstMatchForAllLists )
      {
        // there is a candidate result                       
        newCandidates.add( minValueForAllLists );
        
        // skip the inverted lists to the matched value ( exclude )
        for ( int i = 0; i < minArray.size(); i++ )
        {
          if ( minArray.get( i ) <= minValueForAllLists )
          {
            NavigableSet< Integer > sortedArray = allLists.get( i );
            allLists.set( i, sortedArray.tailSet( minValueForAllLists, false ) );
          }
        }
      }
      else
      {
        // there is no candidate, need to skip lists 
        int minValue = Math.max( minValueForAllLists, minValueForNewLists );
        
        // exceed to upper bound, return
        if ( minValue == Integer.MAX_VALUE )
        {
          return;
        }

        
        if ( oldCandidates.contains( minValue ) )
        {          
          for ( int i = 0; i < minArray.size(); i++ )
          {
            if ( minArray.get( i ) <= minValue )
            {
              NavigableSet< Integer > sortedArray = allLists.get( i );
              allLists.set( i, sortedArray.tailSet( minValue, false ) );
            }
          }
        }
        else
        {
          for ( int i = 0; i < minArray.size(); i++ )
          {
            if ( minArray.get( i ) < minValue )
            {
              NavigableSet< Integer > sortedArray = allLists.get( i );
              allLists.set( i, sortedArray.tailSet( minValue, true ) );
            }
          }
        }        
      }
      
      minArray.clear();
      minHeapForAllLists.clear();
      minHeapForNewLists.clear();
      
    } while ( !isEmptyForNewLists( allLists, newListStartingIndex ) );     
  }
  
  
  
  
  public int continuousNodeJoinAdvanced( 
      ArrayList< NavigableMap< String, Integer >> oldSortedMapList,
      ArrayList< NavigableMap< String, Integer >> newSortedMapList,
      ArrayList< Integer > oldWeightList, 
      ArrayList< Integer > newWeightList, 
      int oldThreshold,
      int newThreshold,
      Collection< String > oldCandidates,
      Collection< String > newCandidates )
  {

    int numberOfNewLists = 0;
    if ( newWeightList != null )
    {
      for ( int weight : newWeightList )
      {
        numberOfNewLists += weight;
      }
    }

    // if there is no new list, no need to run
    if ( numberOfNewLists == 0 )
    {
      return -1;            
    }


    // handle the removed id in three cases:
    // case 1 : the number of additional matches is greater than the number of new lists
    // only need to handle the ids in the oldCandidates
    if ( newThreshold >= oldThreshold + numberOfNewLists )
    {
      this.verifyNodeCandidatesInAllLists( 
        oldSortedMapList, newSortedMapList,
        oldWeightList, newWeightList, 
        oldThreshold, newThreshold,
        oldCandidates, newCandidates );
      
      return 1;
    }

    // case 2: it requires additional matches and the number of matches is smaller than the number
    // of new lists
    // partition the list and merge the partitions
    else if ( newThreshold >= oldThreshold )
    {
      this.continuousNodeJoinForCaseTwo( 
        oldSortedMapList, newSortedMapList, 
        oldWeightList, newWeightList, 
        oldThreshold, newThreshold,
        oldCandidates, newCandidates );
      
      return 2;
    }

    // case 3: newThreshold < oldThreshold
    // restart merging lists from sketch
    else
    {
      ArrayList< NavigableMap< String, Integer >> idList =
          new ArrayList< NavigableMap< String, Integer > >();
      ArrayList< Integer > weightList = new ArrayList< Integer >();

      idList.addAll( oldSortedMapList );
      idList.addAll( newSortedMapList );
      weightList.addAll( oldWeightList );
      weightList.addAll( newWeightList );

      // merge all the inverted lists
      this.nodeJoin( idList, weightList, newThreshold, newCandidates );
      
      return 3;
    }

  }

  
  public void continuousNodeJoinForCaseTwo( 
      ArrayList< NavigableMap< String, Integer >> oldSortedMapList,
      ArrayList< NavigableMap< String, Integer >> newSortedMapList,
      ArrayList< Integer > oldWeightList, 
      ArrayList< Integer > newWeightList, 
      int oldThreshold,
      int newThreshold,
      Collection< String > oldCandidates,
      Collection< String > newCandidates )
  {
    /*
     * for the ids not in the oldCandidates, use the partition method to solve
     */
    ArrayList< NavigableMap< String, Integer > > allLists = new ArrayList< NavigableMap< String, Integer > >();
    allLists.addAll( oldSortedMapList );
    allLists.addAll( newSortedMapList );

    int newListStartingIndex = oldSortedMapList.size();

    ArrayList< Integer > weightList = new ArrayList< Integer >();
    weightList.addAll( oldWeightList );
    weightList.addAll( newWeightList );


    int weightUpperBound = 0;
    for ( int weight : weightList )
    {
      weightUpperBound += weight;
    }

    // the maximum weight is smaller than the threshold
    if ( weightUpperBound < newThreshold )
    {
      return;
    }
    // if the maximum weight is greater than the threshold and there is only one list,
    // directly output the list
    else if ( weightList.size() == 1 )
    {
      NavigableMap< String, Integer > idSet = allLists.get( 0 );

      if ( idSet != null )
      {
        newCandidates.addAll( idSet.keySet() );
      }
      return;
    }

    // for the ids in the oldCandidates, and delete the oldCandidate as well
    this.verifyNodeCandidatesInAllLists( 
        allLists, weightList, oldThreshold, newThreshold,
        oldCandidates, newCandidates );


    // threshold for allList and newLists
    int thresholdForAllLists = newThreshold;
    int thresholdForNewLists = (newThreshold - oldThreshold + 1);

    // start merging the inverted lists
    do
    {
      ArrayList< String > minArray = new ArrayList< String >();

      // two minHeap, One for allLists, the other for the newLists
      TreeSet< String > minHeapForAllLists = new TreeSet< String >();
      TreeSet< String > minHeapForNewLists = new TreeSet< String >();

      // add the min result to the heap
      for ( int i = 0; i < allLists.size(); i++ )
      {
        NavigableMap< String, Integer > sortedArray = allLists.get( i );

        String min;
        if ( sortedArray.isEmpty() )
        {
          // if the map is null, add the maximum value, 
          // in this case, all the node id are smaller than "4", so we use "4" as the maximum value
          min = "4";
        }
        else
        {
          min = sortedArray.firstKey();
        }

        // add it to the min array to record the frontier of each inverted list
        minArray.add( min );
        minHeapForAllLists.add( min );

        if ( i >= newListStartingIndex )
        {
          minHeapForNewLists.add( min );
        }
      }


      // for the new inverted lists
      Iterator< String > minHeapNewItr = minHeapForNewLists.iterator();
      int accumulativeCountForNewLists = 0;
      String minValueForNewLists = "4";

      while ( minHeapNewItr.hasNext() )
      {
        minValueForNewLists = minHeapNewItr.next();
        
        for ( int i = newListStartingIndex; i < minArray.size(); i++ )
        {
          if ( minArray.get( i ).equals( minValueForNewLists ) )
          {
            int weight = weightList.get( i );
            accumulativeCountForNewLists += weight;
            
            if ( accumulativeCountForNewLists >= thresholdForNewLists )
            {
              break;
            } 
          }
        }

        if ( accumulativeCountForNewLists >= thresholdForNewLists )
        {
          break;
        }               
      }


      // for all inverted lists
      Iterator< String > minHeapAllItr = minHeapForAllLists.iterator();      
      boolean isFirstForAllLists = true;
      boolean firstMatchForAllLists = false;
      int accumulativeCountForAllLists = 0;
      String minValueForAllLists = "4";

      while ( minHeapAllItr.hasNext() )
      {
        minValueForAllLists = minHeapAllItr.next();

        // the minimum value;
        if ( isFirstForAllLists )
        {
          isFirstForAllLists = false;
          for ( int i = 0; i < minArray.size(); i++ )
          {
            if ( minArray.get( i ).equals( minValueForAllLists ) )
            {
              int weight = weightList.get( i );              
              accumulativeCountForAllLists += weight;
              
              if ( accumulativeCountForAllLists >= thresholdForAllLists )
              {
                firstMatchForAllLists = true;
                break;
              }
            }
          }

          if ( accumulativeCountForAllLists >= thresholdForAllLists )
          {
            firstMatchForAllLists = true;
            break;
          }
        }

        // if the minimum value does not match
        else
        {
          for ( int i = 0; i < minArray.size(); i++ )
          {
            if ( minArray.get( i ).equals( minValueForAllLists ) )
            {
              int weight = weightList.get( i );
              accumulativeCountForAllLists += weight;
              
              if ( accumulativeCountForAllLists >= thresholdForAllLists )
              {
                break;
              }
            }
          }

          if ( accumulativeCountForAllLists >= thresholdForAllLists )
          {
            break;
          }
        }
      }

                        
      
      if ( ( minValueForNewLists.compareTo( minValueForAllLists ) <= 0 ) &&  firstMatchForAllLists )
      {
        // there is a candidate result
        newCandidates.add( minValueForAllLists );
        
        // skip the inverted lists to the matched value ( exclude )
        for ( int i = 0; i < minArray.size(); i++ )
        {
          if ( minArray.get( i ).compareTo( minValueForAllLists ) <= 0 )
          {
            NavigableMap< String, Integer > sortedArray = allLists.get( i );
            allLists.set( i, sortedArray.tailMap( minValueForAllLists, false ) );                       
          }
        }
      }
      else
      {
        // there is no candidate, need to skip lists
        String minValue;        
        if ( minValueForAllLists.compareTo( minValueForNewLists ) < 0 )
        {
          minValue = minValueForNewLists;
        }
        else
        {
          minValue = minValueForAllLists;
        }        
        
        // exceed to upper bound, return
        if ( minValue.equals( "4" ) )
        {
          return;
        }

        if ( oldCandidates.contains( minValue ) )
        {       
          for ( int i = 0; i < minArray.size(); i++ )
          {
            if ( minArray.get( i ).compareTo( minValue ) <= 0 )
            {
              NavigableMap< String, Integer > sortedArray = allLists.get( i );
              allLists.set( i, sortedArray.tailMap( minValue, false ) );
            }
          }
        }
        else
        {
          for ( int i = 0; i < minArray.size(); i++ )
          {
            if ( minArray.get( i ).compareTo( minValue ) < 0 )
            {
              NavigableMap< String, Integer > sortedArray = allLists.get( i );
              allLists.set( i, sortedArray.tailMap( minValue, true ) );
            }
          }
        }
      }
      
      minArray.clear();
      minHeapForAllLists.clear();
      minHeapForNewLists.clear();
     
    }
    while ( !isEmptyForNewMaps( allLists, newListStartingIndex ) );
  }

  
  
 public void verifyCandidatesInAllLists( 
     ArrayList< NavigableSet< Integer > > oldLists,
     ArrayList< NavigableSet< Integer > > newLists,
     ArrayList< Integer > oldWeightList,
     ArrayList< Integer > newWeightList,
     int oldThreshold,      
     int newThreshold,     
     Collection< Integer > oldCandidates,     
     Collection< Integer > newCandidates )
 {
   ArrayList< NavigableSet< Integer > > allLists = new ArrayList< NavigableSet< Integer > >(); 
   ArrayList< Integer > weightList = new ArrayList< Integer >();
   
   allLists.addAll( oldLists );
   allLists.addAll( newLists );
   weightList.addAll( oldWeightList );
   weightList.addAll( newWeightList );
   
   this.verifyCandidatesInAllLists( allLists, weightList, oldThreshold, newThreshold, oldCandidates, newCandidates );
 }
                                  

  /**
   * CASE 2
   * Need to check all the inverted lists instead of the new invertedLists
   * @param newSortedArrayList
   * @param newWeightList
   * @param oldThreshold
   * @param newThreshold
   * @param oldCandidates
   * @param newCandidates
   */
  public void verifyCandidatesInAllLists( 
      ArrayList< NavigableSet< Integer > > allLists,
      ArrayList< Integer > allWeightList, 
      int oldThreshold, 
      int newThreshold,
      Collection< Integer > oldCandidates,
      Collection< Integer > newCandidates )
  {        
    if ( oldCandidates == null )
    {
      return;
    }
    // if the oldThreshold == newThreshold, directly output the results
    if ( oldThreshold == newThreshold )
    {
      newCandidates.addAll( oldCandidates );
      return;
    }

    // visit each id in the oldCandidates set
    for ( int id : oldCandidates )
    {      
      int count = 0;              
      for ( int i = 0; i < allLists.size(); i++ )
      {
        NavigableSet< Integer > idSet = allLists.get( i );
        int weight = allWeightList.get( i );
        
        if ( idSet.contains( id ) )
        {                    
          count += weight;                  
          if ( count >= newThreshold )
          {
            break;
          }
        }      
      }

      if ( count >= newThreshold )
      {
        newCandidates.add( id );
      }
    }
  }
  
  

  public void verifyNodeCandidatesInAllLists(
      ArrayList< NavigableMap< String, Integer >> oldSortedMapList,
      ArrayList< NavigableMap< String, Integer >> newSortedMapList,
      ArrayList< Integer > oldWeightList, 
      ArrayList< Integer > newWeightList,
      int oldThreshold,
      int newThreshold, 
      Collection< String > oldCandidates,
      Collection< String > newCandidates )
  {
    ArrayList< NavigableMap< String, Integer > > allLists = new ArrayList< NavigableMap< String, Integer > >();
    ArrayList< Integer > weightList = new ArrayList< Integer >();

    allLists.addAll( oldSortedMapList );
    allLists.addAll( newSortedMapList );
    weightList.addAll( oldWeightList );
    weightList.addAll( newWeightList );

    this.verifyNodeCandidatesInAllLists( allLists, weightList, oldThreshold, newThreshold,
        oldCandidates, newCandidates );
  }


  /**
   * CASE 2 Need to check all the inverted lists instead of the new invertedLists
   * 
   * @param newSortedArrayList
   * @param newWeightList
   * @param oldThreshold
   * @param newThreshold
   * @param oldCandidates
   * @param newCandidates
   */
  public void verifyNodeCandidatesInAllLists( 
      ArrayList< NavigableMap< String, Integer > > allLists,
      ArrayList< Integer > allWeightList, 
      int oldThreshold, 
      int newThreshold,
      Collection< String > oldCandidates,
      Collection< String > newCandidates )
  {
    if ( oldCandidates == null )
    {
      return;
    }

    // if the oldThreshold == newThreshold, directly output the results
    if ( oldThreshold == newThreshold )
    {
      newCandidates.addAll( oldCandidates );
      return;
    }

    // visit each id in the oldCandidates set
    for ( String id : oldCandidates )
    {
      int count = 0;
      for ( int i = 0; i < allLists.size(); i++ )
      {
        NavigableMap< String, Integer > map = allLists.get( i );
        int weight = allWeightList.get( i );
        
        if ( map.containsKey( id ) )
        {      
          count += weight;
          if ( count >= newThreshold )
          {
            break;
          }
        }
      }

      if ( count >= newThreshold )
      {
        newCandidates.add( id );
      }
    }
  }

  
 
//
// public void verifyNodeCandidatesInNewLists( 
//     ArrayList< NavigableMap< String, Integer > > newSortedArrayList,
//     ArrayList< Integer > newWeightList, 
//     int oldThreshold,
//     int newThreshold, 
//     Map< String, Integer > oldCandidates,
//     Map< String, Integer > newCandidates)
// {
//   if ( oldCandidates == null )
//   {
//     return;
//   }
//
//   // if the oldThreshold == newThreshold, directly output the results
//   if ( oldThreshold == newThreshold )
//   {
//     newCandidates.putAll( oldCandidates );
//     return;
//   }    
//   
//   // visit each id in the oldCandidates set
//   for ( Entry< String, Integer > candidateEntry : oldCandidates.entrySet() )
//   {
//     String id = candidateEntry.getKey();
//     int oldCount = candidateEntry.getValue();      
//     int requiredAdditionalCount = newThreshold - oldCount;
//           
//
//     // find id in each inverted list,
//     // if found, minutes the corresponding weight for the requiredAdditionalCount
//     for ( int i = 0; i < newSortedArrayList.size(); i++ )
//     {        
//       NavigableMap< String, Integer > idSet = newSortedArrayList.get(i);
//       int weight = newWeightList.get(i);
//              
//       if ( idSet.containsKey( id ) )
//       {
//         requiredAdditionalCount -= weight;         
//       }         
//     }
//           
//     // check whether requiredAddtionalCount <= 0
//     // if yes, add the id into the candidate set
//     if ( requiredAdditionalCount <= 0 )
//     {
//       int count = oldCount - requiredAdditionalCount;        
//       newCandidates.put( id, count );
//     }      
//   }
// }


  
  
  public boolean isEmptyForNewLists ( final ArrayList< NavigableSet< Integer > > sortedArrayList, int newListStartingIndex )
  {    
    for ( int i = newListStartingIndex; i < sortedArrayList.size(); i++ )
    {
      NavigableSet< Integer > array = sortedArrayList.get( i );
      if ( ! array.isEmpty() )
      {
        return false;
      }
    }
    return true;        
  }
  
  
  public boolean isEmptyForNewMaps( final ArrayList< NavigableMap< String, Integer > > sortedArrayList, int newMapStartingIndex )
  {    
    for ( int i = newMapStartingIndex; i < sortedArrayList.size(); i++ )
    {
      NavigableMap< String, Integer > array = sortedArrayList.get( i );
      if ( ! array.isEmpty() )
      {
        return false;
      }
    }
    return true;        
  }
  
  
  


  /*
  public static void main( String[] args )
  {
    TreeSet< Integer > set1 = new TreeSet< Integer >();
    TreeSet< Integer > set2 = new TreeSet< Integer >();
    TreeSet< Integer > set3 = new TreeSet< Integer >();
    TreeSet< Integer > set4 = new TreeSet< Integer >();
    TreeSet< Integer > set5 = new TreeSet< Integer >();
    
    

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
    
    set4.add( 3 );
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
//    int threshold = 4;
    TreeSet< Integer > candidates = new TreeSet< Integer >();
    
    
    
    HashMap< Integer, TreeSet< Integer >> map = new HashMap< Integer, TreeSet< Integer >>();
    map.put( 1, set1 );
    
    
    System.out.println("threshold: " + threshold );
    System.out.println( "set1:" + set1 );
    System.out.println( "set2:" + set2 );
    System.out.println( "set3:" + set3 );
    System.out.println( "set4:" + set4 );
    System.out.println( "set5:" + set5 );
    System.out.println( "map" + map ); 
    
    
    
    
    MergeSkip operator = new MergeSkip();    
    operator.idJoin( arrayList, weightList, threshold, candidates );
    
    System.out.println( "candidates" + candidates );     
//    System.out.println( "set1:" + set1 );
//    System.out.println( "map" + map );
    
    // test for the new lists
    TreeSet< Integer > set11 = new TreeSet< Integer >();
    TreeSet< Integer > set12 = new TreeSet< Integer >();
    TreeSet< Integer > set13 = new TreeSet< Integer >();
    
    set11.add( 3 );
    set11.add( 10 );
    set11.add( 20 );
    set11.add( 30 );
    set11.add( 40 );
    set11.add( 50 );   
    
    set12.add( 30 );
    set12.add( 40 );
    set12.add( 50 );
    set12.add( 60 );
    set12.add( 70 );
    
    
    set13.add( 30 );
    set13.add( 40 );
    set13.add( 50 );
    set13.add( 80 );
    set13.add( 90 );
    
    
    ArrayList< NavigableSet< Integer >> newArrayList = new ArrayList< NavigableSet< Integer >>();    
    newArrayList.add( set11 );
    newArrayList.add( set12 );
    newArrayList.add( set13 );
    
    
    ArrayList< Integer > newWeightList = new ArrayList< Integer >(); 
    
    newWeightList.add( 1 );
    newWeightList.add( 1 );
    newWeightList.add( 1 );
    
    
//  int newThreshold = 2;
//  int newThreshold = 3;
//  int newThreshold = 4;    
  int newThreshold = 5;
//  int newThreshold = 6;
//  int newThreshold = 7;
    

    
    TreeSet< Integer > newCandidates = new TreeSet< Integer >();
    
    ContinuousMergeSkip op = new ContinuousMergeSkip();
    
    arrayList.clear();
    arrayList.add( set1 );
    arrayList.add( set2 );
    arrayList.add( set3 );
    arrayList.add( set4 );
    arrayList.add( set5 );
    
    
    op.continuousIdJoinAdvanced( arrayList, newArrayList, weightList, newWeightList, threshold, newThreshold, candidates, newCandidates );
    
    System.out.println("test for new lists");
    System.out.println("threshold: " + newThreshold );
    System.out.println( "set1:" + set1 );
    System.out.println( "set2:" + set2 );
    System.out.println( "set3:" + set3 );
    System.out.println( "set4:" + set4 );
    System.out.println( "set5:" + set5 );    
    System.out.println( "set6:" + set11 );
    System.out.println( "set7:" + set12 );
    System.out.println( "set8:" + set13 );
    System.out.println( "new candidates" + newCandidates ); 
    
    
    System.out.println( "oldLists: " + arrayList );
    System.out.println( "newLists: " + newArrayList );
    
  }
  */

}
