package storage.objectindex;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.TreeSet;

import unit.PositionalQgram;
import unit.QgramGenerator;
import unit.SpatialObject;
import unit.serializer.SpatialObjectSerializer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class SpatialObjectDatabase {

	public IntegerByteArrayDatabase _db;
	public Kryo _kryo;
	public QgramGenerator _gen;
	public QgramGenerator _largerQgramGen;
	public int _positionBound = -1;
	
	public SpatialObjectDatabase( String fileName, int qgramLength )
	{
		_db = new IntegerByteArrayDatabase(fileName + ".object.db");
		_gen = new QgramGenerator(qgramLength);
		initKryo();
	}
	
	public SpatialObjectDatabase( String fileName, int qgramLength, int largerQgramLength, int positionBound )
	{
		_db = new IntegerByteArrayDatabase(fileName + ".object.db");
		_gen = new QgramGenerator(qgramLength);		
		initKryo();
		
		// 	newly add
		_largerQgramGen = new QgramGenerator(largerQgramLength);
		_positionBound = positionBound;
	}
	
	
	// memory version
	public SpatialObjectDatabase( int qgramLength, int largerQgramLength, int positionBound )
    {
	    // create a memory version database 
        _db = new IntegerByteArrayDatabase();
        _gen = new QgramGenerator(qgramLength);     
        initKryo();
        
        //  newly add
        _largerQgramGen = new QgramGenerator(largerQgramLength);
        _positionBound = positionBound;
    }
	
	
	public void create()
	{
		_db.createTree(Integer.toString(0));

	}
	
	public void load()
	{
		_db.loadTree(Integer.toString(0));
	}
	
	public void initKryo()
	{
		_kryo = new Kryo();
		_kryo.register(SpatialObject.class, new SpatialObjectSerializer(_kryo));
	}
	
	
	public void write(int objectid, SpatialObject object)
	{
//		Output output = new Output(64, 4096);
	  Output output = new Output(8, 4096);
		_kryo.writeObject(output, object);
		_db.write(objectid, output.getBuffer());
	}
	
	
	public SpatialObject getSpatialObject (int objectid)
	{
		byte[] value = _db.read(objectid);
		if ( value == null )
		{
	      System.out.println( "null object id: " + objectid);
	      return null;
		}
			
		else {
			Input input = new Input(value);
			SpatialObject spatialObject = _kryo.readObject(input, SpatialObject.class);
			return spatialObject;
		}
	}
	
	
	
	
	public HashSet<String> getQgramHashSetForLargerQ(int objectid)
	{
		SpatialObject object = getSpatialObject(objectid);
		
		if (object == null)
		{
			return null;
		}
		else
		{
			return _largerQgramGen.getQgramHashSet(object._text);
		}
	}
	
	
	public HashSet<String> getQgramHashSetForLargerQ(SpatialObject object)
	{		
		return _largerQgramGen.getQgramHashSet(object._text);		
	}
	
	
	
	
	public ArrayList<PositionalQgram> getFirstMPositionalQgram( int objectid )
    {
        SpatialObject object = getSpatialObject(objectid);      
        if (object == null)
        {
            return null;
        }
        else
        {
            return _gen.getFirstMPositionalQgramArrayList( object._text, _positionBound );
        }
    }
    
    
    public ArrayList<PositionalQgram> getFirstMPositionalQgram( SpatialObject object )
    {
        return _gen.getFirstMPositionalQgramArrayList( object._text, _positionBound );  
    }
	
    
    public ArrayList<PositionalQgram> getPositionalQgramSet(int objectid)
    {
        SpatialObject object = getSpatialObject(objectid);
        
        if (object == null)
        {
            return null;
        }
        else
        {
            return _gen.getPositionalQgramArrayList(object._text);
        }
    }
        
    public ArrayList<PositionalQgram> getPositionalQgramArrayList(SpatialObject object)
    {
        return _gen.getPositionalQgramArrayList(object._text);   
    }
    
    
	public TreeSet<PositionalQgram> getPositionalQgramSet(SpatialObject object)
	{
		return _gen.getPositionalQgramList(object._text);	
	}
	
	
	public void flush()
	{
		_db.flush();
	}
	
	public void close()
	{
		_db._db.close();
	}
	
}
