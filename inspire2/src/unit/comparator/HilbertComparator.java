package unit.comparator;

import java.io.Serializable;
import java.util.Comparator;

public class HilbertComparator implements Comparator<String>, Serializable {

	private static final long serialVersionUID = 1L;

	public HilbertComparator() {
	
	}

	@Override
	public int compare(String str0, String str1) {
		
//		int len0 = str0.length();
//		int len1 = str1.length();
//		int minLength = Math.min( len0, len1 );
//		
//		for ( int i = 0; i < minLength; i++) 
//		{			
//			char char0 = str0.charAt(i); 
//			char char1 = str1.charAt(i);
//				
//			int charComparedValue = Character.compare(char0, char1);
//			if ( charComparedValue != 0 )
//			{
//				return charComparedValue;
//			}
//			else
//			{
//				continue;
//			}					
//		}
//		
//		return Integer.compare( len0, len1 );
	  
	  
	  return str0.compareTo( str1 );
	  
	}

}
