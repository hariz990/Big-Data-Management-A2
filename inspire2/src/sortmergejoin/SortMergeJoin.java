package sortmergejoin;

import java.util.ArrayList;
import java.util.NavigableSet;
import java.util.TreeSet;

public class SortMergeJoin
{

  public static TreeSet< Integer > join( ArrayList< NavigableSet< Integer >> array )
  {
    if ( array.size() == 1 )
    {
      TreeSet< Integer > set = new TreeSet< Integer > ();
      set.addAll( array.get( 0 ) );
      return set;
    }
    
    int maxOfMin = Integer.MIN_VALUE;
    int minOfMax = Integer.MAX_VALUE;

    for ( NavigableSet< Integer > currentSet : array )
    {
      int currentMin = currentSet.first();
      int currentMax = currentSet.last();

      if ( maxOfMin < currentMin )
      {
        maxOfMin = currentMin;
      }

      if ( minOfMax > currentMax )
      {
        minOfMax = currentMax;
      }
    }

    if ( maxOfMin > minOfMax ) return null;

    ArrayList< NavigableSet< Integer >> tempArray = new ArrayList< NavigableSet< Integer >>();

    for ( NavigableSet< Integer > set : array )
    {
      tempArray.add( set.subSet( maxOfMin, true, minOfMax, true ) );
    }

    return mergeJoin( tempArray );
  }


  private static TreeSet< Integer > mergeJoin( ArrayList< NavigableSet< Integer >> array )
  {
    TreeSet< Integer > resultSet = new TreeSet< Integer >();

    while ( !isOneEmpty( array ) )
    {
      int minOfMin = Integer.MAX_VALUE;
      int maxOfMin = Integer.MIN_VALUE;

      for ( NavigableSet< Integer > currentSet : array )
      {
        int currentMin = currentSet.first();

        if ( minOfMin > currentMin )
        {
          minOfMin = currentMin;
        }

        if ( maxOfMin < currentMin )
        {
          maxOfMin = currentMin;
        }
      }

      // same number, add to result
      if ( minOfMin == maxOfMin )
      {
        resultSet.add( maxOfMin );
        for ( int i = 0; i < array.size(); i++ )
        {
          NavigableSet< Integer > currentSet = array.get( i );
//          currentSet.pollFirst();
          array.set( i, currentSet.tailSet( maxOfMin, false ) );
        }
      }
      else
      {

        for ( int i = 0; i < array.size(); i++ )
        {
          NavigableSet< Integer > currentSet = array.get( i );
          if ( currentSet.first() != maxOfMin )
          {
            array.set( i, currentSet.tailSet( maxOfMin, true ) );
          }
        }
      }
    }

    return resultSet.isEmpty() ? null : resultSet;
  }


  private static boolean isOneEmpty( final ArrayList< NavigableSet< Integer >> array )
  {
    for ( NavigableSet< Integer > set : array )
    {
      if ( set == null || set.isEmpty() ) return true;
    }
    return false;
  }

}
