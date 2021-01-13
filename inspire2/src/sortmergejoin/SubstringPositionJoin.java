package sortmergejoin;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.NavigableSet;
import java.util.TreeSet;

import unit.PositionalQgram;

public class SubstringPositionJoin extends SortMergeJoin{

	
	public static TreeSet<Integer> getPossibleStartPosition( HashMap<PositionalQgram, NavigableSet<Integer>> posMap)
	{				
		ArrayList<NavigableSet<Integer>> tempPositionListVector = new ArrayList<NavigableSet<Integer>> ();		
		Iterator<Entry<PositionalQgram, NavigableSet<Integer>>> itr = posMap.entrySet().iterator();
		
		while ( itr.hasNext() )
		{
			Entry<PositionalQgram, NavigableSet<Integer>> entry = itr.next();
			int relativePosition = entry.getKey()._pos;
			NavigableSet<Integer> positionList = entry.getValue();
			
			if ( relativePosition == 0 ) 
			{
				tempPositionListVector.add(positionList);
			} 
			else 
			{
				TreeSet<Integer> newPositionList = new TreeSet<Integer>();
				for ( int pos : positionList ) 
				{
					if ( pos >= relativePosition )
					{
						newPositionList.add( pos - relativePosition );
					}
				}
				
				if ( newPositionList .isEmpty() )
				{
					return null;
				}
				else
				{
					tempPositionListVector.add(newPositionList);
				}
			}			
		}
		
		return join(tempPositionListVector);
	}
	
	
}
