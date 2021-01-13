package storage.invertedindex;

import java.util.Iterator;
import java.util.TreeSet;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentNavigableMap;

import org.apache.jdbm.DB;
import org.apache.jdbm.DBMaker;

import unit.IDSet;
import unit.PositionalQgram;
import unit.PositionalQgramInvertedList;

public class InfrequentQgramTokenInvertedIndex {

	public DB _db;
	public ConcurrentNavigableMap<String, int[]> _map;
	public int _count;
//	public int commitFrequency = 10000000;
	public int commitFrequency = Integer.MAX_VALUE;
	
	
	// disk version
	public InfrequentQgramTokenInvertedIndex(String fileName)
	{
		_db = DBMaker.openFile(fileName + ".infrequent.token.index").make();
	}
		
	// memory version
	public InfrequentQgramTokenInvertedIndex()
    {
        _db = DBMaker.openMemory().make();
    }
	
	public void creatTree() {	
		_map = _db.createTreeMap( 
				Integer.toString( 0 ), 
				null, null, null);		
	}
	
	public void loadTree() {
		_map = _db.getTreeMap( Integer.toString(0) );
	}
	
	public void deleteTree(String treeName) {
		_db.deleteCollection(treeName);
	}

	public void flush() 
	{	
		_db.commit();
	}
	
	public void put(String key, int[] value)
	{						
		_map.put(key, value);		
		if(_count % commitFrequency == 0) {
			_db.commit();
		}
		_count++;		
	}
	
	public void put(String key, IDSet value)
	{						
		put(key, value.getIntegerArray());		
	}
	
	public void put(PositionalQgramInvertedList invertedList)
	{
		Iterator<Entry<PositionalQgram, IDSet>> itr = invertedList._table.entrySet().iterator();
		while ( itr.hasNext() )
		{
			Entry<PositionalQgram, IDSet> entry = itr.next();
			put(entry.getKey().toString(), entry.getValue());
		}
	}
	
	
	public int[] read(String key)
	{
		int[] value = _map.get(key);
		if(value == null){	
			return null;
		}			
		return value;
	}
	
		
//	public int[] getPositionList(PositionalQgram qgram)
//	{
//		return read(qgram._qgram);
//	}
	
	
//	public int[] getIDSet(PositionalQgram qgram)
//	{
//		String key;
//		if ( qgram._pos != -1 )
//		{
//			key = qgram.toString();
//		} 
//		else 
//		{
//			key = qgram._qgram;
//		}
//		return _map.get(key);
//	}
	
	
	
	   public TreeSet< Integer > getIdSet(String qgramToken)
	    {	     
	      int[] idSet = _map.get( qgramToken );
	      if (  idSet == null )
	      {
	        return null;
	      }
	      else
	      {
	        TreeSet< Integer > result = new TreeSet< Integer > ();
	        for ( int id : idSet )
	        {
	          result.add( id );	          
	        }
	        return result;
	      }
	    }
	
	
	
	public ConcurrentNavigableMap<String, int[]> getInfrequentQgramInvertedMap(
			final String fromQgram,
			final String toQgram) 
	{		
		return _map.subMap(fromQgram, false, toQgram, false);		
	}
	
	
	
	
	
	public boolean containsKey(String key)
	{
		return _map.containsKey(key);
	}
	
	
	public void remove(String key)
	{
		_map.remove(key);
	}
	
	
	public void clearCache(){
		_db.clearCache();
	}
	
	
	public void resetIO(){
	}	
	
}
