package unit.comparator;

import java.io.Serializable;
import java.util.Comparator;

public class PositionalQgramStringComparator implements Comparator<String> , Serializable{

	private static final long serialVersionUID = 1L;
	
	public int _qgramLength;
	
	public PositionalQgramStringComparator(int qgramLength)
	{
		_qgramLength = qgramLength;
	}
	
	@Override
	public int compare(String str0, String str1) 
	{
		
		String qgram0 = str0.substring(0, _qgramLength);
		String qgram1 = str1.substring(0, _qgramLength);
		
		int tokenComparedValue = qgram0.compareTo(qgram1);
		
		//	if tokens are different
		if ( tokenComparedValue != 0 ) 
		{
			return tokenComparedValue; 
		}
		//	if tokens are the same
		else 
		{			
			String posStr0 =  str0.substring(_qgramLength);
			String posStr1 =  str1.substring(_qgramLength);
			
			int pos0 = (posStr0.length() == 0) ? -(_qgramLength + 1) : Integer.parseInt( posStr0 );
			int pos1 = (posStr1.length() == 0) ? -(_qgramLength + 1) : Integer.parseInt( posStr1 );
			
			if ( pos0 < pos1 )
				return -1;
			else if ( pos0 == pos1 )
				return 0;
			else 
				return 1;
		}
		
	}

}
