package unit.comparator;

import java.io.Serializable;
import java.util.Comparator;


public class SecondLevelKeyStringComparator implements Comparator<String> , Serializable{

	private static final long serialVersionUID = 1L;
	private int _gramLength;
	private HilbertComparator _hibertComp;
	private PositionalQgramStringComparator _gramComp;
	
	public SecondLevelKeyStringComparator(int gramLength)
	{
		this._gramLength = gramLength;
		_hibertComp = new HilbertComparator();
		_gramComp = new PositionalQgramStringComparator(this._gramLength);
	}
	
	
	@Override
	public int compare(String str0, String str1)
	{
		//	split the string to the hilbert code part and positional q-gram part
		String [] tempStr0 = str0.split( ",", 2 );
		String [] tempStr1 = str1.split( ",", 2 );
		
		
		//	Hilbert code comparison
		int locValue = _hibertComp.compare( tempStr0[0], tempStr1[0] );
		
		if ( locValue != 0 ) 
		{
			return locValue;
		}
		else 
		{
//			if ( tempStr0.length < tempStr1.length) 
//			{
//				return -1;
//			} 
//			else if ( tempStr0.length > tempStr1.length) 
//			{
//				return 1;
//			} 
//			else if ( tempStr0.length == 0) 
//			{
//				return 0;
//			}
//			else 
//			{
//				return this._gramComp.compare(tempStr0[1], tempStr1[1]);
//			}	
			
			return _gramComp.compare( tempStr0[1], tempStr1[1] );
		}
	}

}
