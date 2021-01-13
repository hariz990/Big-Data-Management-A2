package unit;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeMap;

import unit.comparator.HilbertComparator;

public class HilbertCountMap {
	
	public TreeMap<String, Integer> _map;
	
	
	public HilbertCountMap()
	{
		this._map = new TreeMap<String, Integer> ( new HilbertComparator() );
	}
	
	@Override
	public String toString()
	{
		return _map.toString();
	}
	
	
	public boolean isEmpty()
	{
	  return _map.isEmpty();
	}
	
	
	public HilbertCountMap getNewCopy()
	{
	  HilbertCountMap newCopy = new HilbertCountMap();	  	  
	  Iterator< Entry < String, Integer > > itr = this._map.entrySet().iterator();
	  
	  while ( itr.hasNext() )
	  {
	    Entry < String, Integer > entry = itr.next();
	    newCopy._map.put( entry.getKey(), entry.getValue() );
	  }
	  	  
	  return newCopy;
	}
}
