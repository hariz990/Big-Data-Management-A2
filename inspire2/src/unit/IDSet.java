package unit;

import java.io.Serializable;
import java.util.Iterator;
import java.util.TreeSet;

public class IDSet implements Serializable {
	private static final long serialVersionUID = 1L;
	
	public TreeSet<Integer> idset;
	
	public IDSet()
	{
		idset = new TreeSet<Integer>();
	}
	
	public IDSet(int id)
	{
		idset = new TreeSet<Integer>();
		idset.add(id);
	}
	
	public IDSet(int[] ids)
	{
		idset = new TreeSet<Integer>();
		for (int i = 0; i < ids.length; i++) {
			idset.add(ids[i]);
		}
	}
	
	public IDSet(IDSet set)
	{
		idset = new TreeSet<Integer>();
		idset.addAll(set.idset);
	}
	
	public void add(int id)
	{
		idset.add(id);
	}
	
	public boolean contains ( int id )
	{
		return idset.contains(id);
	}
	
	
	public int[] getIntegerArray()
	{	
		int[] result = new int[idset.size()];
		
		Iterator<Integer> itr = idset.iterator();
		int idx = 0;
		while ( itr.hasNext() )
		{
			result[idx++] = itr.next();
		}	
		return result;
	}

	public void addAll ( IDSet tempIDSet ) 
	{
		idset.addAll(tempIDSet.idset);
		
	}
	
	public void intersect ( IDSet set )
	{
		idset.retainAll(set.idset);
	}

	public int size() {
		return idset.size();
	}
	
	public boolean isEmpty()
	{
		return idset.isEmpty();
	}
	
	
	@Override
	public String toString()
	{
		return idset.toString();
	}
}
