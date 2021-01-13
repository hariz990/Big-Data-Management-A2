package unit.comparator;

import java.util.Comparator;

import unit.PositionalQgram;

public class PositionalQgramComparator implements Comparator<PositionalQgram> {

	
	public PositionalQgramComparator()
	{
		
	}
	
	@Override
	public int compare(PositionalQgram qgram0, PositionalQgram qgram1) 
	{				
		if ( qgram0 == null && qgram1 == null )
		{
			return 0;
		}
		else if ( qgram0 == null )
		{
			return -1;
		}
		else if ( qgram1 == null )
		{
			return 1;
		}
		else 
		{
			int tokenComparedValue = qgram0._qgram.compareTo(qgram1._qgram);
			
			if ( tokenComparedValue != 0 )
			{
				return tokenComparedValue;
			} 
			else 
			{
				return Integer.compare( qgram0._pos, qgram1._pos );
			}	
		}			
	}

}
