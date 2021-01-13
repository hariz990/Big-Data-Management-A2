package unit;

import java.util.HashSet;
import java.util.Hashtable;

public class PositionalQgramInvertedList {

	public Hashtable<PositionalQgram, IDSet> _table;
	
	public PositionalQgramInvertedList()
	{
		_table = new Hashtable<PositionalQgram, IDSet>();
	}
	
	public void add(PositionalQgram qgram, int id)
	{
		IDSet set = _table.get(qgram);
		if ( set == null ) {
			set = new IDSet(id);
			_table.put(qgram, set);
		} else {
			set.add(id);
		}
	}
	
	public HashSet<PositionalQgram> getQgramSet()
	{		
		HashSet<PositionalQgram> resultSet = new HashSet<PositionalQgram>();
		resultSet.addAll(_table.keySet());
		return resultSet;
	}
	
}
