package unit.comparator;

import java.util.Comparator;

import unit.SecondLevelKey;

public class SecondLevelKeyComparator implements Comparator<SecondLevelKey> {

	HilbertComparator nodeComparator = null;
	PositionalQgramComparator positionalQgramComparator = null;
	
	public SecondLevelKeyComparator()
	{
		nodeComparator = new HilbertComparator();
		positionalQgramComparator = new PositionalQgramComparator();
	}
	
	
	@Override
	public int compare(SecondLevelKey key0, SecondLevelKey key1) {
			
		int nodeComparedValue = nodeComparator.compare( key0._hilbertCode, key1._hilbertCode );
		
		if ( nodeComparedValue != 0 )
		{
			return nodeComparedValue;
		}
		else 
		{	
			return positionalQgramComparator.compare( key0._qgram, key1._qgram );
		}
	}
	
	
	/*
	
	public static void main( String[] args )
	{
		SecondLevelKeyComparator comparator = new SecondLevelKeyComparator();
		
		
		int length = 1;
		char maxChar = Character.MAX_VALUE ;
		char testChar = '?';
		
		System.out.println( maxChar );
		System.out.println( testChar );
		System.out.println( maxChar > testChar );
		
		char[] charArray = new char[length];
		for ( int i = 0; i < length; i ++ )
		{
			charArray[i] = maxChar;
		}
		String str = new String( charArray );
		System.out.println( str );
		
		System.out.println( str.compareTo(str) );
		
//		SecondLevelKey key0 = new SecondLevelKey( "001", null ); 
//		SecondLevelKey key1 = new SecondLevelKey( "001", null );
//		
//		System.out.println( comparator.compare(key0, key1) );
	}
	
	 */
}
