package unit.comparator;

import java.io.Serializable;
import java.util.Comparator;


public class HilbertQgramTokenComparator implements Comparator<String>, Serializable {

	private static final long serialVersionUID = 4860311630192605561L;
	
	private HilbertComparator _hibertComp;
	
	public HilbertQgramTokenComparator()
	{
		_hibertComp = new HilbertComparator();
	}
	
	@Override
	public int compare(String str0, String str1) {		
		
//		split the string to the hilbert code part and positional q-gram part			
		String[] tempStr0 = str0.split(",", 2);
		String[] tempStr1 = str1.split(",", 2);

		// Hilbert code comparison
		int hilbertComparedValue = _hibertComp.compare(tempStr0[0], tempStr1[0]);

		if (hilbertComparedValue != 0) 
		{
			return hilbertComparedValue;
		}
		else 
		{
			return tempStr0[1].compareTo(tempStr1[1]);
		}		
	}

}
