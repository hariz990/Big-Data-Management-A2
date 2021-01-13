package storage.invertedindex;

import java.util.TreeSet;
import java.util.concurrent.ConcurrentNavigableMap;

import org.apache.jdbm.DB;
import org.apache.jdbm.DBMaker;

import unit.PositionalQgram;
import unit.SecondLevelKey;
import unit.comparator.SecondLevelKeyStringComparator;

public class SecondLevelInvertedIndex {
	
	public ConcurrentNavigableMap<String, int[]> _map;
	public DB _database;
	public int _qgramLength;
	public int count = 0;
//	public int commitFrequency = 10000;
	public int commitFrequency = Integer.MAX_VALUE;
	
	
	// disk version
	public SecondLevelInvertedIndex( String filename, int qgramLength )
	{
		_database = DBMaker.openFile(filename + ".second.inverted.index").make();
		_qgramLength = qgramLength;
	}

	// memory version
	public SecondLevelInvertedIndex( int qgramLength )
    {
        _database = DBMaker.openMemory().make();
        _qgramLength = qgramLength;
    }
	
	
	public void createMap()
	{
		_map = _database.createTreeMap(
				Integer.toString(0), 
				new SecondLevelKeyStringComparator(_qgramLength), 
				null, null );
	}
	
	public void loadMap()
	{
		_map = _database.getTreeMap( Integer.toString(0) );
	}
	
	public void put(String key, int[] value)
	{
		_map.put(key, value);
		count ++;
		
		if ( count % commitFrequency == 0 ) {
		  _database.commit();
		}
	}
	
	public int[] readPositionList( String hilbertCode, String gram )
	{
		return _map.get( hilbertCode + gram );
	}
	
	public int[] readObjectList( String hilbertCode, PositionalQgram positionalQgram )
	{
		return _map.get( hilbertCode + "," + positionalQgram.toString() );
	}
	
	
	public TreeSet<Integer> readObjectList( SecondLevelKey key )
    {
        int[] idSet = _map.get( key.toString() );
        if ( idSet == null )
        {
          return null;
        }
        else
        {
          TreeSet<Integer> ids = new TreeSet<Integer>();
          for ( int id : idSet )
          {
            ids.add( id );
          }
          return ids;
        }          
    }
	
  public TreeSet< Integer > readObjectList( String key )
  {
    int[] idSet = _map.get( key );
    if ( idSet == null )
    {
      return null;
    }
    else
    {
      TreeSet< Integer > ids = new TreeSet< Integer >();
      for ( int id : idSet )
      {
        ids.add( id );
      }
      return ids;
    }
  }
	
	
//	public TreeMap<String, TreeSet<Integer>> getInvertedMap (
//			final PositionalQgram fromQgram,
//			final PositionalQgram toQgram,
//			final String hilbertCode,
//			final TreeSet<Integer> startPositions,
//			final int relativePosition) 
//	{					
//		String fromKey = hilbertCode + "," + fromQgram; 
//		String toKey = hilbertCode + "," + toQgram;
//		
//		// get the submap 
//		ConcurrentNavigableMap<String, int[]> invertedMap = 
//			_map.subMap( fromKey, true, toKey, true);
//		
//		TreeMap<String, TreeSet<Integer>> resultMap = null;
//			
//		
//		if ( invertedMap != null )
//		{
//			resultMap = new TreeMap<String, TreeSet<Integer>>(new KeyComparator(_qgramLength));
//			
//			Iterator<Entry<String, int[]>> itr = invertedMap.entrySet().iterator();
//			if ( itr.hasNext() )
//			{
//				Entry<String, int[]> entry = itr.next();
//				String key = entry.getKey();
//				
//				int pos = Integer.parseInt( key.substring( key.length() - _qgramLength ) );
//				if ( startPositions.contains( pos - relativePosition ) )
//				{
//					int[] integerArray = entry.getValue();
//					
//					TreeSet<Integer> objectIdList = new TreeSet<Integer>();
//					for ( int objectid : integerArray )
//					{
//						objectIdList.add(objectid);
//					}
//					
//					// store it in result
//					resultMap.put( key, objectIdList );
//				}		
//			}
//			
//			return resultMap.isEmpty() ? null : resultMap;
//		}
//		else {
//			return null;
//		}			
//	}
	
	
	public ConcurrentNavigableMap<String, int[]> getInvertedMap (
			final String hilbertCode,
			final String fromQgram,
			final String toQgram ) 
	{					
		String fromKey = hilbertCode + "," + fromQgram; 
		String toKey = hilbertCode + "," + toQgram;
		
		// get the submap 
		return _map.subMap( fromKey, true, toKey, true);
	}
	
	
	public ConcurrentNavigableMap<String, int[]> getInvertedMap (
			final String hilbertCode,
			final PositionalQgram fromPositionalQgram,
			final PositionalQgram toPositionalQgram ) 
	{					
		String fromKey = hilbertCode + "," + fromPositionalQgram.toString(); 
		String toKey = hilbertCode + "," + toPositionalQgram.toString();
		
		// get the submap 
		return _map.subMap( fromKey, true, toKey, true);
	}
	
	
	
	public void flush()
	{
		_database.commit();
	}
}
