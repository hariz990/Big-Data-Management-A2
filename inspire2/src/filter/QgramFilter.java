package filter;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.TreeSet;

import unit.QgramGenerator;

public class QgramFilter {

  public int qgramLength;
  public QgramGenerator generator;
  

  public QgramFilter( int qgramLength )
  {
    this.qgramLength = qgramLength;
    this.generator = new QgramGenerator( qgramLength );
  }
  
  public QgramFilter( QgramGenerator generator )
  {
    this.generator = generator;
    this.qgramLength = generator._len;
  }
  

  public boolean approximateFullStringFilter( final String queryText,
      final TreeMap< String, TreeSet< Integer > > queryQgramTokenPositiontMap,
      final TreeMap< String, TreeSet< Integer > > prefixQueryQgramTokenPositiontMap,
      final String objectText, final int tau )
  {
    /**
     * check length
     */
    if ( Math.abs( queryText.length() - objectText.length() ) > tau )
    {
      return false;
    }
        
    int minMatchThreshold = Math.max( queryText.length(), objectText.length() ) - qgramLength + 1 - ( tau * qgramLength );

    // if the minMatchThreshold is less than 0, it has no filtering power
    if ( minMatchThreshold <= 0 )      
    {
      return true;
    }
    
    /**
     *  prefix filter 
     */
    int prefixBound = ( tau * qgramLength + 1 ) + qgramLength - 1;
    
    TreeMap< String, TreeSet< Integer > > prefixObjectQgramPositionMap = 
        generator.getTokenPositionMap( objectText.substring( 0, prefixBound ) );
    
    
    // if the query text or the object text is too short
    if ( prefixQueryQgramTokenPositiontMap == null || prefixObjectQgramPositionMap == null )
    {
      return true;
    }
    
    if ( this.positionalQgramPrefixFilter( prefixQueryQgramTokenPositiontMap, prefixObjectQgramPositionMap, tau ) )
    {      
      /**
       *  q-gram token count filter      
       */
      TreeMap< String, TreeSet< Integer > > objectQgramPositionMap = generator.getTokenPositionMap( objectText ); 
      
      int matchedQgramTokenCount = this.getMatchingQgramTokenCountNumber( queryQgramTokenPositiontMap, objectQgramPositionMap );
      if ( matchedQgramTokenCount >= minMatchThreshold )
      {       
        /**
         *  positional q-gram count filter
         */
        int matchedPositionalQgramCount = this.getPrefixMatchingPosQgramCountNumber( queryQgramTokenPositiontMap, objectQgramPositionMap, tau );
        if ( matchedPositionalQgramCount >= minMatchThreshold )
        {
          return true;
        }
        
      }         
    }
    
    
    return false;
  }
  
  
  public boolean approximatePrefixFilter( final String queryText, 
      final TreeMap< String, TreeSet< Integer > > queryQgramTokenPositiontMap,
      final TreeMap< String, TreeSet< Integer > > prefixQueryQgramTokenPositiontMap,
      final String objectText,
      final int tau )
  {
    /**
     * check length
     */
    if ( queryText.length() - objectText.length() > tau )
    {
      return false;
    }    
    
    int minMatchThreshold = queryText.length() - qgramLength + 1 - ( tau * qgramLength );
    
    // if the minMatchThreshold is less than 0, it has no filtering power
    if ( minMatchThreshold <= 0 )      
    {
      return true;
    }
    
    /**
     *  prefix filter 
     */
//    int prefixBound = ( tau * qgramLength ) + 1;
    int prefixBound = ( tau * qgramLength + 1 ) + qgramLength - 1;
    int toIndex = Math.min( prefixBound, objectText.length() );
    
    TreeMap< String, TreeSet< Integer > > prefixObjectQgramPositionMap = generator.getTokenPositionMap( objectText.substring( 0, toIndex ) );
    
    
    // if the query text or the object text is too short
    if ( prefixQueryQgramTokenPositiontMap == null || prefixObjectQgramPositionMap == null )
    {
      return true;
    }
    
    if ( this.positionalQgramPrefixFilter( prefixQueryQgramTokenPositiontMap, prefixObjectQgramPositionMap, tau ) )
    {      
      /**
       *  q-gram token count filter      
       */
      TreeMap< String, TreeSet< Integer > > objectQgramPositionMap = generator.getTokenPositionMap( objectText ); 
      
      int matchedQgramTokenCount = this.getMatchingQgramTokenCountNumber( queryQgramTokenPositiontMap, objectQgramPositionMap );
      if ( matchedQgramTokenCount >= minMatchThreshold )
      {       
        /**
         *  positional q-gram count filter
         */
        int matchedPositionalQgramCount = this.getPrefixMatchingPosQgramCountNumber( queryQgramTokenPositiontMap, objectQgramPositionMap, tau );
        if ( matchedPositionalQgramCount >= minMatchThreshold )
        {
          return true;
        }
        
      }         
    }
       
    return false;
  }

  
  
  public boolean approximatePrefixPositionalQgramFilter( final String queryText,
      final TreeMap< String, TreeSet< Integer > > queryQgramTokenPositiontMap,
      final TreeMap< String, TreeSet< Integer > > prefixQueryQgramTokenPositiontMap,
      final String objectText, final int tau )
  {
    int minMatchThreshold = queryText.length() - qgramLength + 1 - (tau * qgramLength);

    // if the minMatchThreshold is less than 0, it has no filtering power
    if ( minMatchThreshold <= 0 )
    {
      return true;
    }

    TreeMap< String, TreeSet< Integer > > objectQgramPositionMap =
        generator.getTokenPositionMap( objectText );

    int matchedQgramTokenCount =
        this.getMatchingQgramTokenCountNumber( queryQgramTokenPositiontMap, objectQgramPositionMap );
    if ( matchedQgramTokenCount >= minMatchThreshold )
    {
      /**
       * positional q-gram count filter
       */
      int matchedPositionalQgramCount =
          this.getPrefixMatchingPosQgramCountNumber( queryQgramTokenPositiontMap,
              objectQgramPositionMap, tau );
      if ( matchedPositionalQgramCount >= minMatchThreshold )
      {
        return true;
      }
    }
    return false;
  }
  
  
  
  
  public boolean approximateSubstringFilter( final String queryText,
      final TreeMap< String, TreeSet< Integer > > queryQgramTokenPositiontMap,
      final String objectText,
      TreeSet< Integer > startPositionCandidates,
      final int tau )
  {
    /**
     * check length
     */
    if ( queryText.length() - objectText.length() > tau )
    {
      return false;
    }    
    
    int minMatchThreshold = queryText.length() - qgramLength + 1 - ( tau * qgramLength );
    
    // if the minMatchThreshold is less than 0, it has no filtering power
    if ( minMatchThreshold <= 0 )      
    {
      return true;
    }
    
    /**
     * q-gram token count filter
     */
    TreeMap< String, TreeSet< Integer > > objectQgramPositionMap =
        generator.getTokenPositionMap( objectText );
    
    // if the query text or the object text is too short
    if ( queryQgramTokenPositiontMap == null || objectQgramPositionMap == null )
    {
      return true;
    }

    int matchedQgramTokenCount = this.getMatchingQgramTokenCountNumber( queryQgramTokenPositiontMap, objectQgramPositionMap );    
    if ( matchedQgramTokenCount >= minMatchThreshold )
    {
     
      /**
       * get the possible start positions
       */
      this.getSubstringStartPositionCandidates( 
          queryQgramTokenPositiontMap, objectQgramPositionMap, startPositionCandidates, tau, minMatchThreshold );
      if ( startPositionCandidates.isEmpty() )
      {
        return false;
      }
      else
      {   
        /**
         * filter the start positions
         */
        Iterator< Integer > itr = startPositionCandidates.iterator();
        while ( itr.hasNext() )
        {
          int startPosition = itr.next();
          int matchingPosQgramCount = this.getSubstringMatchingPosQgramCountNumberAtStartPosition( 
              queryQgramTokenPositiontMap, objectQgramPositionMap, tau, startPosition );
          
          if ( matchingPosQgramCount < minMatchThreshold )
          {
            itr.remove();
          }
        }
        
        if ( startPositionCandidates.isEmpty() )
        {
          return false;
        }
        else
        {
          return true;
        }        
      }      
    }    
    return false;    
  }
  
  

  public boolean approximateSubstringFilterWithoutCountFilter( final String queryText,
      final TreeMap< String, TreeSet< Integer > > queryQgramTokenPositiontMap,
      final String objectText, TreeSet< Integer > startPositionCandidates, final int tau )
  {
    /**
     * check length
     */
    if ( queryText.length() - objectText.length() > tau )
    {
      return false;
    }

    int minMatchThreshold = queryText.length() - qgramLength + 1 - (tau * qgramLength);

    // if the minMatchThreshold is less than 0, it has no filtering power
    if ( minMatchThreshold <= 0 )
    {
      return true;
    }

    /**
     * q-gram token count filter
     */
    TreeMap< String, TreeSet< Integer > > objectQgramPositionMap =
        generator.getTokenPositionMap( objectText );

    // if the query text or the object text is too short
    if ( queryQgramTokenPositiontMap == null || objectQgramPositionMap == null )
    {
      return true;
    }

    /**
     * get the possible start positions
     */
    this.getSubstringStartPositionCandidates( queryQgramTokenPositiontMap, objectQgramPositionMap,
        startPositionCandidates, tau, minMatchThreshold );
    
    if ( startPositionCandidates.isEmpty() )
    {
      return false;
    }
    else
    {
      /**
       * filter the start positions
       */
      Iterator< Integer > itr = startPositionCandidates.iterator();
      while ( itr.hasNext() )
      {
        int startPosition = itr.next();
        int matchingPosQgramCount =
            this.getSubstringMatchingPosQgramCountNumberAtStartPosition(
                queryQgramTokenPositiontMap, objectQgramPositionMap, tau, startPosition );

        if ( matchingPosQgramCount < minMatchThreshold )
        {
          itr.remove();
        }
      }

      if ( startPositionCandidates.isEmpty() )
      {
        return false;
      }
      else
      {
        return true;
      }
    }
  }
  
 

  
  private boolean positionalQgramPrefixFilter(
      final TreeMap< String, TreeSet< Integer > > prefixQueryQgramPositionMap,            
      final TreeMap< String, TreeSet< Integer > > prefixObjectQgramPositionMap,      
      int tau)
  {
    
    Iterator< Entry< String, TreeSet< Integer > > > queryItr =
        prefixQueryQgramPositionMap.entrySet().iterator();
    Iterator< Entry< String, TreeSet< Integer > > > objectItr =
        prefixObjectQgramPositionMap.entrySet().iterator();

    Entry< String, TreeSet< Integer > > queryEntry = queryItr.next();
    Entry< String, TreeSet< Integer > > objectEntry = objectItr.next();

    String queryToken = queryEntry.getKey();
    TreeSet< Integer > queryTokenPositionSet = queryEntry.getValue();

    String objectToken = objectEntry.getKey();
    TreeSet< Integer > objectTokenPositionSet = objectEntry.getValue();
    int tokenComparedValue;

    while ( true )
    {
      tokenComparedValue = queryToken.compareTo( objectToken );

      // token matches
      if ( tokenComparedValue == 0 )
      {        
        // check whether there is a matching
        
        Iterator< Integer > queryTokenPositionItr = queryTokenPositionSet.iterator();
        Iterator< Integer > objectTokenPositionItr = objectTokenPositionSet.iterator();
        
        int queryTokenPosition = queryTokenPositionItr.next();
        int objectTokenPosition = objectTokenPositionItr.next();
        int positionComparedValue;
        
        while ( true )
        {
          // compute the position match value
          if ( queryTokenPosition < objectTokenPosition - tau )
          {
            positionComparedValue = -1;
          }
          else if ( (queryTokenPosition >= objectTokenPosition - tau)
              && (queryTokenPosition <= objectTokenPosition + tau) )
          {
            // positional q-grams match, return true
            return true;
          }
          else
          {
            positionComparedValue = 1;
          }
   
          // position not match, query position moves
          if ( positionComparedValue < 0 )
          {
            if ( queryTokenPositionItr.hasNext() )
            {
              queryTokenPosition = queryTokenPositionItr.next();
            }
            else
            {
              break;
            }
          }
          // position not match, object position moves
          else if ( positionComparedValue > 0 )
          {
            if ( objectTokenPositionItr.hasNext() )
            {
              objectTokenPosition = objectTokenPositionItr.next();
            }
            else
            {
              break;
            }
          }    
        }
                              
        
        if ( queryItr.hasNext() && objectItr.hasNext() )
        {
          queryEntry = queryItr.next();
          queryToken = queryEntry.getKey();
          queryTokenPositionSet = queryEntry.getValue();

          objectEntry = objectItr.next();
          objectToken = objectEntry.getKey();
          objectTokenPositionSet = objectEntry.getValue();
        }
        else
        {
          break;
        }
      }
      
      // queryItr moves
      else if ( tokenComparedValue < 0 )
      {
        if ( queryItr.hasNext() )
        {
          queryEntry = queryItr.next();
          queryToken = queryEntry.getKey();
          queryTokenPositionSet = queryEntry.getValue();
        }
        else
        {
          break;
        }
      }
      
      // objectItr moves
      else
      {
        if ( objectItr.hasNext() )
        {
          objectEntry = objectItr.next();
          objectToken = objectEntry.getKey();
          objectTokenPositionSet = objectEntry.getValue();

        }
        else
        {
          break;
        }
      }
    }
    
    return false;
           
  }
  

  
  
  private int getMatchingQgramTokenCountNumber(
      final TreeMap< String, TreeSet< Integer > > queryQgramPositionMap,
      final TreeMap< String, TreeSet< Integer > > objectQgramPositionMap )
  {
    int numOfMatch = 0;
    Iterator< Entry< String, TreeSet< Integer > > > queryItr =
        queryQgramPositionMap.entrySet().iterator();
    Iterator< Entry< String, TreeSet< Integer > > > objectItr =
        objectQgramPositionMap.entrySet().iterator();

    Entry< String, TreeSet< Integer > > queryEntry = queryItr.next();
    Entry< String, TreeSet< Integer > > objectEntry = objectItr.next();

    String queryToken = queryEntry.getKey();
    int queryTokenCount = queryEntry.getValue().size();

    String objectToken = objectEntry.getKey();
    int objectTokenCount = objectEntry.getValue().size();
    int tokenComparedValue;

    while ( true )
    {
      tokenComparedValue = queryToken.compareTo( objectToken );

      // match
      if ( tokenComparedValue == 0 )
      {
        numOfMatch += Math.min( queryTokenCount, objectTokenCount );

        if ( queryItr.hasNext() && objectItr.hasNext() )
        {
          queryEntry = queryItr.next();
          queryToken = queryEntry.getKey();
          queryTokenCount = queryEntry.getValue().size();

          objectEntry = objectItr.next();
          objectToken = objectEntry.getKey();
          objectTokenCount = objectEntry.getValue().size();
        }
        else
        {
          break;
        }
      }
      // queryItr moves
      else if ( tokenComparedValue < 0 )
      {
        if ( queryItr.hasNext() )
        {
          queryEntry = queryItr.next();
          queryToken = queryEntry.getKey();
          queryTokenCount = queryEntry.getValue().size();
        }
        else
        {
          break;
        }
      }
      // objectItr moves
      else
      {
        if ( objectItr.hasNext() )
        {
          objectEntry = objectItr.next();
          objectToken = objectEntry.getKey();
          objectTokenCount = objectEntry.getValue().size();
        }
        else
        {
          break;
        }
      }
    }
    return numOfMatch;
  }
  
  
  private int getPrefixMatchingPosQgramCountNumber(
      final TreeMap< String, TreeSet< Integer > > queryQgramPositionMap,
      final TreeMap< String, TreeSet< Integer > > objectQgramPositionMap,
      final int tau )
  {
    return getSubstringMatchingPosQgramCountNumberAtStartPosition( 
      queryQgramPositionMap, objectQgramPositionMap, tau, 0 ); 
  }
      
  
  
  private void getSubstringStartPositionCandidates(
      final TreeMap< String, TreeSet< Integer > > queryQgramPositionMap,
      final TreeMap< String, TreeSet< Integer > > objectQgramPositionMap,
      TreeSet< Integer > startPositionCandidates,
      final int tau,
      final int minMatchThreshold )
  {
    if ( queryQgramPositionMap == null || objectQgramPositionMap == null )
    {
      return ;
    }
    else
    {
      // store the total start count entry.
      HashMap< Integer, Integer > totalStartCountMap = new HashMap< Integer, Integer >();
            
      Iterator< Entry< String, TreeSet< Integer > > > queryItr = queryQgramPositionMap.entrySet().iterator();
      Iterator< Entry< String, TreeSet< Integer > > > objectItr = objectQgramPositionMap.entrySet().iterator();

      Entry< String, TreeSet< Integer > > queryEntry = queryItr.next();
      Entry< String, TreeSet< Integer > > objectEntry = objectItr.next();

      String queryToken = queryEntry.getKey();
      TreeSet< Integer > queryTokenPositionSet = queryEntry.getValue();

      String objectToken = objectEntry.getKey();
      TreeSet< Integer > objectTokenPositionSet= objectEntry.getValue();
      int tokenComparedValue;

      while ( true )
      {
        tokenComparedValue = queryToken.compareTo( objectToken );

        // token match
        if ( tokenComparedValue == 0 )
        {          
          // the upper bound of the token match value
          int tokenMatchBound = Math.min( queryTokenPositionSet.size(), objectTokenPositionSet.size() );

          // token start count map          
          HashMap< Integer, Integer > tokenStartCountMap = new HashMap< Integer, Integer >();
          
          // for each object position, get its relative start positions
          for ( int objectPosition : objectTokenPositionSet )
          {            
            HashSet< Integer > startSet = new HashSet< Integer >();
            
            // get the relative start position for each query position
            for ( int queryPosition : queryTokenPositionSet )
            {
              int exactStart = objectPosition - queryPosition;
              for ( int i = ( -tau ) ; i <= tau ; i++ )
              {
                int relaxedStart =  exactStart + i;                
                if ( relaxedStart >= 0 )
                {
                  startSet.add( relaxedStart );
                }                
              }              
            }
            
            // accumulate the start positions for the query position
            for ( int start : startSet )
            {
              Integer count = tokenStartCountMap.get( start );
              if ( count == null )
              {
                tokenStartCountMap.put( start, 1 );                
              }
              else
              {
                tokenStartCountMap.put( start, count + 1 );
              }              
            }            
          }
          
          // accumulate the start positions for this token
          Iterator< Entry < Integer, Integer >> startItr = tokenStartCountMap.entrySet().iterator();
          while ( startItr.hasNext() )
          {
            Entry< Integer, Integer > startEntry = startItr.next();
            int tokenStart = startEntry.getKey();
            int tokenCount = startEntry.getValue();
            
            // the to-add count value is not larger than the token match bound value
            int toAddCount = Math.min( tokenCount, tokenMatchBound );
                        
            Integer totalCount = totalStartCountMap.get( tokenStart );
            if ( totalCount == null )
            {
              totalStartCountMap.put( tokenStart, toAddCount );
            }
            else
            {
              totalStartCountMap.put( tokenStart, totalCount + toAddCount );
            }                  
          }

          // get the next token
          if ( queryItr.hasNext() && objectItr.hasNext() )
          {
            queryEntry = queryItr.next();
            queryToken = queryEntry.getKey();
            queryTokenPositionSet = queryEntry.getValue();

            objectEntry = objectItr.next();
            objectToken = objectEntry.getKey();
            objectTokenPositionSet = objectEntry.getValue();
          }
          else
          {
            break;
          }
        }
        
        // token not match, queryItr moves
        else if ( tokenComparedValue < 0 )
        {
          if ( queryItr.hasNext() )
          {
            queryEntry = queryItr.next();
            queryToken = queryEntry.getKey();
            queryTokenPositionSet = queryEntry.getValue();
          }
          else
          {
            break;
          }
        }
        
        // token not match, objectItr moves
        else
        {
          if ( objectItr.hasNext() )
          {
            objectEntry = objectItr.next();
            objectToken = objectEntry.getKey();
            objectTokenPositionSet = objectEntry.getValue();

          }
          else
          {
            break;
          }
        }
      }
      
      
      // browse the totalStartCountMap, and get the start position with count greater than the minMatchThreshold
      Iterator< Entry< Integer, Integer > > itr = totalStartCountMap.entrySet().iterator();
      while ( itr.hasNext() )
      {
        Entry< Integer, Integer > entry = itr.next();
        int start = entry.getKey();
        int count = entry.getValue();
        if ( count >= minMatchThreshold )
        {
          startPositionCandidates.add( start );
        }
        itr.remove();
      }      
    }    
  }
  
  
  
  private int getSubstringMatchingPosQgramCountNumberAtStartPosition(
      final TreeMap< String, TreeSet< Integer > > queryQgramPositionMap,
      final TreeMap< String, TreeSet< Integer > > objectQgramPositionMap, 
      final int tau,
      final int startPos)
  {
    if ( queryQgramPositionMap == null || objectQgramPositionMap == null )
    {
      return 0;
    }
    else
    {
      int numOfMatch = 0;
      Iterator< Entry< String, TreeSet< Integer > > > queryItr =
          queryQgramPositionMap.entrySet().iterator();
      Iterator< Entry< String, TreeSet< Integer > > > objectItr =
          objectQgramPositionMap.entrySet().iterator();

      Entry< String, TreeSet< Integer > > queryEntry = queryItr.next();
      Entry< String, TreeSet< Integer > > objectEntry = objectItr.next();

      String queryToken = queryEntry.getKey();
      TreeSet< Integer > queryTokenPositionSet = queryEntry.getValue();

      String objectToken = objectEntry.getKey();
      TreeSet< Integer > objectTokenPositionSet = objectEntry.getValue();
      int tokenComparedValue;

      while ( true )
      {
        tokenComparedValue = queryToken.compareTo( objectToken );

        // token match
        if ( tokenComparedValue == 0 )
        {
          int positionComparedValue;
          Iterator< Integer > queryPosItr = queryTokenPositionSet.iterator();
          Iterator< Integer > objectPosItr = objectTokenPositionSet.iterator();
          int queryTokenPosition = queryPosItr.next() + startPos;
          int objectTokenPosition = objectPosItr.next();

          while ( true )
          {
            // compute the position match value
            if ( queryTokenPosition < objectTokenPosition - tau )
            {
              positionComparedValue = -1;
            }
            else if ( (queryTokenPosition >= objectTokenPosition - tau)
                && (queryTokenPosition <= objectTokenPosition + tau) )
            {
              positionComparedValue = 0;
            }
            else
            {
              positionComparedValue = 1;
            }


            // position match
            if ( positionComparedValue == 0 )
            {
              // accumulate the number of matched positional q-gram
              numOfMatch++;

              if ( queryPosItr.hasNext() && objectPosItr.hasNext() )
              {
                queryTokenPosition = queryPosItr.next() + startPos;
                objectTokenPosition = objectPosItr.next();
              }
              else
              {
                break;
              }
            }
            // position not match, query position moves
            else if ( positionComparedValue < 0 )
            {
              if ( queryPosItr.hasNext() )
              {
                queryTokenPosition = queryPosItr.next() + startPos ;
              }
              else
              {
                break;
              }
            }
            // position not match, object position moves
            else
            {
              if ( objectPosItr.hasNext() )
              {
                objectTokenPosition = objectPosItr.next();
              }
              else
              {
                break;
              }
            }
          }

          if ( queryItr.hasNext() && objectItr.hasNext() )
          {
            queryEntry = queryItr.next();
            queryToken = queryEntry.getKey();
            queryTokenPositionSet = queryEntry.getValue();

            objectEntry = objectItr.next();
            objectToken = objectEntry.getKey();
            objectTokenPositionSet = objectEntry.getValue();
          }
          else
          {
            break;
          }

        }
        // token not match, queryItr moves
        else if ( tokenComparedValue < 0 )
        {
          if ( queryItr.hasNext() )
          {
            queryEntry = queryItr.next();
            queryToken = queryEntry.getKey();
            queryTokenPositionSet = queryEntry.getValue();
          }
          else
          {
            break;
          }
        }
        // token not match, objectItr moves
        else
        {
          if ( objectItr.hasNext() )
          {
            objectEntry = objectItr.next();
            objectToken = objectEntry.getKey();
            objectTokenPositionSet = objectEntry.getValue();

          }
          else
          {
            break;
          }
        }
      }

      return numOfMatch;
    }
  }
  
  
  
//	private void objectMaxMissMatchFilter(
//			Hashtable<Integer, Integer> matchHashtable,
//			Hashtable<Integer, Integer> missMatchHashtable, 
//			int maxMissMatch) 
//	{	
//		Iterator<Entry<Integer, Integer>> countItr = missMatchHashtable.entrySet().iterator();
//		while(countItr.hasNext()){
//			Entry<Integer, Integer> countEntry = countItr.next();
//			int child = countEntry.getKey();
//			int count = countEntry.getValue();
//			
//			if (count > maxMissMatch) {
//				// remove it
//				countItr.remove();
//				matchHashtable.remove(child);
//			}
//		}
//	}
//	
//	
//	
//	private TreeSet<Integer> objectMinMatchFilter(
//			Hashtable<Integer, Integer> matchHashtable, 
//			int minMatch) {
//		
//		TreeSet<Integer> childList = new TreeSet<Integer>();
//		
//		Iterator<Entry<Integer, Integer>> countItr = matchHashtable.entrySet().iterator();
//		while(countItr.hasNext()){
//			Entry<Integer, Integer> countEntry = countItr.next();
//			int child = countEntry.getKey();
//			int count = countEntry.getValue();
//			
//			if (count >= minMatch) {
//				childList.add(child);
//			}
//		}
//		return childList.isEmpty() ? null : childList;
//	}	
//	
//	
//	
//	
//	private void nodeMaxMissMatchFilter(
//			Hashtable<String, Integer> matchHashtable,
//			Hashtable<String, Integer> missMatchHashtable, 
//			int maxMissMatch) 
//	{	
//		Iterator<Entry<String, Integer>> countItr = missMatchHashtable.entrySet().iterator();
//		while(countItr.hasNext()){
//			Entry<String, Integer> countEntry = countItr.next();
//			String node = countEntry.getKey();
//			int count = countEntry.getValue();
//			
//			if (count > maxMissMatch) {
//				// remove it
//				countItr.remove();
//				matchHashtable.remove(node);
//			}
//		}
//	}
//	
//	
//	
//	private TreeSet<String> nodeMinMatchFilter(
//			Hashtable<String, Integer> matchHashtable, 
//			int minMatch) {
//		
//		TreeSet<String> childList = new TreeSet<String>();
//		
//		Iterator<Entry<String, Integer>> countItr = matchHashtable.entrySet().iterator();
//		while(countItr.hasNext()){
//			Entry<String, Integer> countEntry = countItr.next();
//			String node = countEntry.getKey();
//			int count = countEntry.getValue();
//			
//			if (count >= minMatch) {
//				childList.add(node);
//			}
//		}
//		return childList.isEmpty() ? null : childList;
//	}
	
	
}
