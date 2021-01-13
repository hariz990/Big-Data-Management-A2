package storage.invertedindex;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentNavigableMap;

import org.apache.jdbm.DB;
import org.apache.jdbm.DBMaker;

import unit.HilbertCountMap;
import unit.IDSet;
import unit.NodeStatistic;
import unit.PositionalQgram;
import unit.comparator.PositionalQgramStringComparator;
import unit.serializer.HilbertCountMapSerializer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class FirstLevelInvertedIndex {

    public DB _db;
	public ConcurrentNavigableMap<String, byte[]> _map;
	public int _count;
	public Kryo _kryo;
	public int _qgramLength = 2;
	
//	public int commitFrequency = 10000;
	public int commitFrequency = Integer.MAX_VALUE;
	
	// use in one gram inverted index
	public FirstLevelInvertedIndex( boolean flag )
	{
		initKryo();
	}
	
	// disk version
	public FirstLevelInvertedIndex(String fileName)
	{
		_db = DBMaker.openFile( fileName + ".first.inverted.index" ).make();
		initKryo();
	}
	
	// memory version
	public FirstLevelInvertedIndex()
    {
        _db = DBMaker.openMemory().make();
        initKryo();
    }
	
	// disk version
	public FirstLevelInvertedIndex(String fileName, int gramLength)
	{
		this(fileName);
		this._qgramLength = gramLength;
	}
	
	// memory version
	public FirstLevelInvertedIndex(int gramLength)
    {
        this();
        this._qgramLength = gramLength;
    }
	
	public void createTree() {	
//		_map = _db.createTreeMap( Integer.toString(0) , 	
//				new PositionalQgramStringComparator( _qgramLength ), 
//				null, null);
	  
	  _map = _db.createTreeMap( Integer.toString(0) ,  
        new PositionalQgramStringComparator( _qgramLength ), 
        null, null);    
	  

	}
	
	public void loadTree() {
		_map = _db.getTreeMap( Integer.toString(0) );
	}
	
	public void deleteTree(String treeName) {
		_db.deleteCollection( treeName );
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
	
	
	public void write(PositionalQgram qgram, HilbertCountMap countMap)
	{						
//		Output output = new Output(64, 4096);
		// -1 for no maximum buffer size
		Output output = new Output(0, -1);
		_kryo.writeObject(output, countMap);
		_map.put(qgram.toString(), output.getBuffer());
		
		if(_count % commitFrequency == 0)
			_db.commit();
		_count++;
	}
	
	public void write(String posQgram, HilbertCountMap countMap)
	{						
//		Output output = new Output(64, 4096);
		// -1 for no maximum buffer size
		Output output = new Output(0, -1);
		_kryo.writeObject(output, countMap);
		_map.put(posQgram, output.getBuffer());
		
		if(_count % commitFrequency == 0)
			_db.commit();
		_count++;
	}
	
	
  public void write( String posQgram, Map< String, Integer > map )
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
    _map.put( posQgram, output.getBuffer() );

    if ( _count % commitFrequency == 0 ) {
      _db.commit();
    }
    _count++;
  }

	
	public void writePositionList (String qgram, IDSet idSet)
	{					
		_map.put( qgram, parseIntegerArrayToString(idSet.getIntegerArray()).getBytes() );
		
		if(_count % commitFrequency == 0) {
			_db.commit();
		}
		
		_count++;
	}
	
	
	public HilbertCountMap read ( PositionalQgram qgram )
	{
		byte[] value = _map.get( qgram.toString() );
		if (value == null) {
			return null;
		}
		else 
		{
			Input input = new Input(value);
			HilbertCountMap countMap = _kryo.readObject(input, HilbertCountMap.class);
			return countMap;
		}			
	}
	
	
	public HilbertCountMap read ( String positionalQgram )
	{
		byte[] value = _map.get( positionalQgram );
		if (value == null) {
			return null;
		}
		else 
		{
			Input input = new Input(value);
			HilbertCountMap countMap = _kryo.readObject(input, HilbertCountMap.class);
			return countMap;
		}			
	}
	
	
	public void getInvertedMap (
			final PositionalQgram fromQgram,
			final PositionalQgram toQgram,
			final TreeMap<String, NodeStatistic> intersectingNodeStatistic,
			final TreeSet<Integer> startPositions,
			final int relativePosition,
			TreeMap<PositionalQgram, HilbertCountMap> memoryFirstLevelInvertedMap) 
	{
		ConcurrentNavigableMap<String, byte[]> invertedMap = 
			_map.subMap( fromQgram.toString(), true, toQgram.toString(), true);
		
		if ( invertedMap != null )
		{
			Iterator<Entry<String, byte[]>> itr = invertedMap.entrySet().iterator();
			while ( itr.hasNext() )
			{
				Entry<String, byte[]> entry = itr.next();
				String gramString = entry.getKey();				
				int pos = Integer.parseInt( gramString.substring(_qgramLength) );
				
				if ( startPositions.contains( pos - relativePosition ) )
				{
					byte[] byteArray = entry.getValue();
					
					PositionalQgram pgram = new PositionalQgram( 
							gramString.substring(0, _qgramLength), 
							Integer.parseInt(gramString.substring(_qgramLength)) );
					
					Input input = new Input(byteArray);
					
					// deserialize the object
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
							memoryFirstLevelInvertedMap.put(pgram, countMap);
						}						
					}					
				}
			}
		}
	}
	
	public void getInvertedMap (
			final PositionalQgram fromQgram,
			final PositionalQgram toQgram,
			final TreeMap<String, NodeStatistic> intersectingNodeStatistic,
			final TreeSet<Integer> startPositions,
			final TreeSet<Integer> relativePositions,
			TreeMap<PositionalQgram, HilbertCountMap> memoryFirstLevelInvertedMap) 
	{
		ConcurrentNavigableMap<String, byte[]> invertedMap = 
			_map.subMap( fromQgram.toString(), true, toQgram.toString(), true);
		
		if ( invertedMap != null )
		{
			Iterator<Entry<String, byte[]>> itr = invertedMap.entrySet().iterator();
			while ( itr.hasNext() )
			{
				Entry<String, byte[]> entry = itr.next();
				String gramString = entry.getKey();
				
				int pos = Integer.parseInt( gramString.substring(_qgramLength) );
				
				for ( int relativePosition : relativePositions )
				{
					if ( startPositions.contains( pos - relativePosition ) )
					{
						if ( startPositions.contains( pos - relativePosition ) )
						{													
							PositionalQgram pgram = new PositionalQgram( 
									gramString.substring(0, _qgramLength), 
									Integer.parseInt(gramString.substring(_qgramLength)) );
							
							// save it to memory
							byte[] byteArray = entry.getValue();
							Input input = new Input(byteArray);
							
							// deserialize the object
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
										memoryFirstLevelInvertedMap.put(pgram, countMap);
								}						
							}
						}
					}
				}
			}
		}
	}
	
	public int[] readPositionList ( String qgram )
	{
		if ( qgram.length() != this._qgramLength ) {
			return null;
		}
		
		byte[] byteArray = _map.get( qgram );
		if ( byteArray == null ) {
			return null;
		}
		else 
		{
			return parseStringToIntegerArray( new String( byteArray ) );
		}	
	}
	
	
	protected String parseIntegerArrayToString(int[] array)
	{
		String result = "[";
		for (int i = 0; i < array.length; i++) {		    		  
			if ( i != array.length -1) {
				result += array[i] + ",";
			} else {
				result += array[i];
			}
		}
		result += "]";
		return result;
	}
	
	
	protected int[] parseStringToIntegerArray(String str)
	{
		String[] items = str.replaceAll("\\[", "").replaceAll("\\]", "").split(",");

		int[] results = new int[items.length];

		for (int i = 0; i < items.length; i++) {
		    try 
		    {
		        results[i] = Integer.parseInt(items[i]);
		    } catch (NumberFormatException nfe) 
		    {
		    	nfe.printStackTrace();
		    };
		}
		
		return results;
	}
}
