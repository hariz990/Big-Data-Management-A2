package storage.invertedindex;

import java.util.NavigableMap;
import java.util.TreeSet;

import org.apache.jdbm.DB;
import org.apache.jdbm.DBMaker;

import unit.IDSet;
import unit.comparator.HilbertQgramTokenComparator;

public class HilbertQgramTokenInvertedIndex
{

  public DB _db;
  public NavigableMap< String, int[] > _map;
  public int _count;
//  public int commitFrequency = 10000;
  public int commitFrequency = Integer.MAX_VALUE;


  // disk version
  public HilbertQgramTokenInvertedIndex(String fileName)
  {
    _db = DBMaker.openFile( fileName + ".hilbert.token.inverted.index" ).make();
  }
  
  public HilbertQgramTokenInvertedIndex()
  {
    _db = DBMaker.openMemory().make();
  }

  public void createTree()
  {
    _map = _db.createTreeMap( Integer.toString( 0 ), new HilbertQgramTokenComparator(), null, null );
  }

  public void loadTree()
  {
    _map = _db.getTreeMap( Integer.toString( 0 ) );
  }

  public void deleteTree( String treeName )
  {
    _db.deleteCollection( treeName );
  }

  public void flush()
  {
    _db.commit();
  }

  public void write( String key, int[] value )
  {
    _map.put( key, value );
    if ( _count % commitFrequency == 0 ) 
      _db.commit();
    _count++;
  }

  public void write( String key, IDSet value )
  {
    _map.put( key, value.getIntegerArray() );
    if ( _count % commitFrequency == 0 ) 
      _db.commit();
    _count++;
  }


  public int[] read( String key )
  {
    int[] value = _map.get( key );
    if ( value == null )
    {
      return null;
    }
    return value;
  }

  
  public TreeSet< Integer > getIdSet( String key )
  {
    int[] value = _map.get( key );
    if ( value == null )
    {
      return null;
    }
    
    TreeSet< Integer > idSet = new TreeSet< Integer >();
    for ( int id : value )
    {
      idSet.add( id );
    }
        
    return idSet;
  }
  
  
  
  public int[] read( String hilbertCode, String token )
  {
    String key = hilbertCode + "," + token;
    int[] value = _map.get( key );
    if ( value == null )
    {
      return null;
    }
    return value;
  }

}
