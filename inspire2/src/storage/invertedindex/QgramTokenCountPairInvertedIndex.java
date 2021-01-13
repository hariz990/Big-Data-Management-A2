package storage.invertedindex;

import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Map.Entry;

import org.apache.jdbm.DB;
import org.apache.jdbm.DBMaker;

import unit.HilbertCountMap;
import unit.serializer.HilbertCountMapSerializer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class QgramTokenCountPairInvertedIndex {

	public DB _db;
	public NavigableMap<String, byte[]> _map;
	public Kryo _kryo;
	public int _count;
//	public int commitFrequency = 10000;
	public int commitFrequency = Integer.MAX_VALUE;
	
	public QgramTokenCountPairInvertedIndex(String fileName)
	{
		_db = DBMaker.openFile(fileName + ".count.pair.inverted.index").make();
		initKryo();
	}
	
	// memory version
	public QgramTokenCountPairInvertedIndex()
    {
	  _db = DBMaker.openMemory().make();
        initKryo();
    }
	
	public void createTree() {	
		_map = _db.createTreeMap(Integer.toString(0));	
	}
	
	public void loadTree() {
		_map = _db.getTreeMap(Integer.toString(0));
	}
	
	public void deleteTree(String treeName) {
		_db.deleteCollection(treeName);
	}

	public void flush() 
	{	
		_db.commit();
	}
	
	public void initKryo()
	{
		_kryo = new Kryo();
		_kryo.register(HilbertCountMap.class, new HilbertCountMapSerializer(_kryo));
	}
	
	
	public void write(String token, HilbertCountMap countMap)
	{						
		Output output = new Output(0, -1);
		_kryo.writeObject(output, countMap);
		_map.put(token, output.getBuffer());
		
		if ( _count % commitFrequency == 0 ) {
			_db.commit();
		}
		_count++;	
	}
	
	
  public void write( String token, Map<String, Integer> map )
  {
    // copy the map to the hilbertCountMap
    HilbertCountMap countMap = new HilbertCountMap();
    Iterator< Entry< String, Integer >> itr = map.entrySet().iterator();
    while ( itr.hasNext() )
    {
      Entry< String, Integer > entry = itr.next();
      countMap._map.put( entry.getKey(), entry.getValue() );
    }

    Output output = new Output( 64, -1 );
    _kryo.writeObject( output, countMap );
    _map.put( token, output.getBuffer() );

    if ( _count % commitFrequency == 0 ) { 
      _db.commit();
    }
    _count++;
  }
	
	
	public HilbertCountMap read(String token)
	{
		byte[] value = _map.get(token);
		if(value == null)
		{	
			return null;
		}			
		else
		{
			Input input = new Input(value);
			HilbertCountMap countMap = _kryo.readObject(input, HilbertCountMap.class);
			return countMap;
		}
	}
	
}
