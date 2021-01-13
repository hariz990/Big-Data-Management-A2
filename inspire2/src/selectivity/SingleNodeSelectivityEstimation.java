package selectivity;

import java.util.ArrayList;



public class SingleNodeSelectivityEstimation {

	public static double getSelectivity( 
			final double intersectRatio, 
			final int[] gramCounts, 
			final double objectNumber )
	{
		double result = intersectRatio * objectNumber;
		for ( int count : gramCounts)
		{
			result *= count/objectNumber;
		}
		return result;
	}
	
	
	public static double getSelectivity( final int[] gramCounts, final double objectNumber )
	{
		return getSelectivity( 1.0, gramCounts, objectNumber);
	}
	
	
	
	public static double getSelectivity( 
			final double intersectRatio, 
			final ArrayList<Integer> gramCounts, 
			final double objectNumber )
	{
		double result = intersectRatio * objectNumber;
		for ( int count : gramCounts)
		{
			result *= count/objectNumber;
		}
		return result;
	}
	
	
	
	public static double getSelectivity( final ArrayList<Integer> gramCounts, final double objectNumber )
	{
		return getSelectivity( 1.0, gramCounts, objectNumber);
	}
	

	
	public static double getSelectivity( 
			final double intersectRatio,
			final ArrayList<Integer> gramCounts,
			final ArrayList<Integer> denominatorList )
	{
		double result = intersectRatio;
		for ( int count : gramCounts )
		{
			result *= count; 
		}
		
		for ( int denominator : denominatorList )
		{
			result /= denominator;
		}	
		return result;
	}
	
	
	
	
	/**
	 * 
	 * @param gramCounts
	 * @return the minimum value of the integer vector
	 */
	public static int getMinValue( final ArrayList<Integer> gramCounts )
	{
		int min = Integer.MAX_VALUE;
		for ( int number : gramCounts )
		{
			if ( min > number )
			{
				min = number;
			}
		}		
		return min;
	}
	
	
	
	public static double getSubstringSelectivityAtPosition( 
			final ArrayList<Integer> gramCounts,
			final ArrayList<Integer> denominators,
			final double objectNumber )
	{
		double result = 1.0;
		
		for ( int count : gramCounts )
		{
			result *= count;
		}
		
		for ( int denominator : denominators )
		{
			result /= denominator;
		}
		
		return ( result / objectNumber ) ;
	}
	
	
	
}
