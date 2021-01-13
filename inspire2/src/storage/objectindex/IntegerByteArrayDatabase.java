package storage.objectindex;

import java.util.NavigableMap;

import org.apache.jdbm.DB;
import org.apache.jdbm.DBMaker;

public class IntegerByteArrayDatabase {
	public DB _db;
	public NavigableMap<Integer, byte[]> _map;
	public int _count;
//	public int commitFrequency = 10000;
	public int commitFrequency = Integer.MAX_VALUE;
	
	public IntegerByteArrayDatabase(String fileName)
	{
		_db = DBMaker.openFile(fileName).make();
	}
	
	public IntegerByteArrayDatabase()
    {
        _db = DBMaker.openMemory().make();
    }
	
	public void createTree(String treeName) {	
		_map = _db.createTreeMap(treeName);	
	}
	
	public void loadTree(String treeName) {
		_map = _db.getTreeMap(treeName);
	}
	
	public void deleteTree(String treeName) {
		_db.deleteCollection(treeName);
	}

	public void flush() 
	{	
		_db.commit();
	}
	
	public void write(int key, byte[] value)
	{						
		_map.put(key, value);		
		if(_count % commitFrequency == 0)
			_db.commit();
		_count++;		
		
	}
	
	
	public byte[] read(int key)
	{
		byte[] value = _map.get(key);
		if(value == null){	
			return null;
		}			
		return value;
	}
	
	
}
