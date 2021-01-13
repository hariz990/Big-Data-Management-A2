package storage.invertedindex;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentNavigableMap;

import org.apache.jdbm.DBMaker;

import unit.HilbertCountMap;
import unit.NodeStatistic;

import com.esotericsoftware.kryo.io.Input;

public class OneGramInvertedIndex extends FirstLevelInvertedIndex {

	
	public OneGramInvertedIndex(String fileName)
	{
		super( true );
		super._db = DBMaker.openFile( fileName + ".onegram.inverted.index" ).make();
		this._qgramLength = 1;
	}
	
	
	public void getPrefixInvertedMap (
			final String key,
			final TreeMap<String, NodeStatistic> intersectingNodeStatistic,
			HashMap< String, HilbertCountMap > denominatorMap )
	{
		HilbertCountMap hilbertCountMap = super.read(key);
		
		if ( hilbertCountMap != null )
		{
			Iterator<Entry<String, Integer>> nodeItr = hilbertCountMap._map.entrySet().iterator();
			while( nodeItr.hasNext() )
			{
				Entry<String, Integer> nodeEntry = nodeItr.next();
				String nodeHilbertCode = nodeEntry.getKey();
					
				if ( ! intersectingNodeStatistic.containsKey(nodeHilbertCode) )
				{
					nodeItr.remove();
				}
			}
			
			//	if it is not empty, store it in memoryFirstLevelInvertedMap
			if ( ! hilbertCountMap._map.isEmpty() )
			{
				// store it in result							
				denominatorMap.put( key, hilbertCountMap );
			}			
		}
		
	}
	
	public void getSubstringInvertedMap (
			final String fromKey,
			final String toKey,
			final TreeSet<Integer> startPositions,
			final TreeMap<String, NodeStatistic> intersectingNodeStatistic,			
			HashMap<String, HilbertCountMap> denominatorMap) 
	{
		ConcurrentNavigableMap<String, byte[]> invertedMap = 
			_map.subMap( fromKey, true, toKey, true);
		
		if ( invertedMap != null )
		{
			Iterator<Entry<String, byte[]>> itr = invertedMap.entrySet().iterator();
			while ( itr.hasNext() )
			{
				Entry<String, byte[]> entry = itr.next();
				String gramString = entry.getKey();				
										
				// deserialize the object
				byte[] byteArray = entry.getValue();	
				Input input = new Input(byteArray);	
				HilbertCountMap countMap = _kryo.readObject(input, HilbertCountMap.class);
					
				if ( countMap != null )
				{
					// not the intersecting nodes are needed
					Iterator<Entry<String, Integer>> nodeItr = countMap._map.entrySet().iterator();
					while ( nodeItr.hasNext() )
					{
						Entry<String, Integer> nodeEntry = nodeItr.next();
						String nodeHilbertCode = nodeEntry.getKey();
							
						if ( ! intersectingNodeStatistic.containsKey(nodeHilbertCode) )
						{
							nodeItr.remove();
						}
					}
						
					//	if it is not empty, store it in memoryFirstLevelInvertedMap
					if ( ! countMap._map.isEmpty() )
					{
						// store it in result							
						denominatorMap.put( gramString, countMap );
					}						
				}					
			}
		}
	}
	
}
