package stringverification;

import java.util.TreeSet;

/*
 * The Knuth-Morris-Pratt Algorithm for Pattern Matching
 */
public class KMPMatch {
  
	private String pattern;
    private int[] next;
    int matchPoint;
  
	public KMPMatch(String pattern) {
		this.pattern = pattern;
        next = new int[pattern.length()];      
		computeFailure();
	}

	public int getMatchPoint() {
		return matchPoint;
	}
  
  
	public boolean match(String text) {
		int M = pattern.length();
		int N = text.length();
		int i, j;
		
		for ( i = 0, j = 0; i < N && j < M; i++ ) {
			while (j >= 0 && text.charAt(i) != pattern.charAt(j)) {
				j = next[j];
			}
			j++;
		}
		if (j == M)
		{
			matchPoint = i - M;
			return true;
		}
		return false;
	}
	
	/**
	 * check whether a text contains a pattern given a set of possible start positions
	 * @param text
	 * @param startPositions
	 * @return
	 */
	public boolean match( String text, TreeSet<Integer> startPositions )
	{
		int M = pattern.length();
		int N = text.length();
		int i, j;
		Integer start = startPositions.first();
		
		for ( i = start, j = 0; i < N && j < M; i++ ) 
		{
			while (j >= 0 && text.charAt(i) != pattern.charAt(j)) 
			{
				j = next[j];
				
				// not match, get the next possible start position
				start = startPositions.ceiling(i - j);
				
				if ( start == null )
				{
					return false;
				}
				else 
				{
					// if current i is less than the start position, join to the start position
					if ( i < start - 1)
					{
						i = start - 1;
						j = -1;
					}
				}
			}
			j++;
		}
		if (j == M)
		{
			matchPoint = i - M;
			return true;
		}
		return false;
	}


	private void computeFailure() {
		int j = -1;
		for (int i = 0; i < pattern.length(); i++) {
			if (i == 0) {
				next[i] = -1;
			} else if (pattern.charAt(i) != pattern.charAt(j)) {
				next[i] = j;
			} else {
				next[i] = next[j];
			}
			while (j >= 0 && pattern.charAt(i) != pattern.charAt(j)) {
				j = next[j];
			}
			j++;
		}
	}
  
  public static void main(String[] args)
  {
//	  String str1 = "abcdefghijklmnopqrstuvwxyz";
//	  String str2 = "abc";
//	  String str3 = "xyz";
//	  String str4 = "klmno";
	  
	  
	  String str1 = "ababababababababababc";
	  String str2 = "ababc";
	  String str3 = "abb";
	  String str4 = "aabaaa";
	  
//	  KMPMatch match1 = new KMPMatch(str1);
	  KMPMatch match2 = new KMPMatch(str2);
	  KMPMatch match3 = new KMPMatch(str3);
	  KMPMatch match4 = new KMPMatch(str4);
	  
	  
	  TreeSet<Integer> posSet = new TreeSet<Integer>();
	  posSet.add(0);
	  posSet.add(6);
	  posSet.add(10);
//	  posSet.add(15);
	  posSet.add(16);
//	  posSet.add(17);
	  
	  
//	  System.out.println(str1.)
	  System.out.println( match2.match(str1, posSet) );
	  
//	  System.out.println( match1.match(str1) );
//	  System.out.println( match2.match(str1) );
//	  System.out.println( match3.match(str1) );
//	  System.out.println( match4.match(str1) );

	  
	  
//	  KMPMatch match5 = new KMPMatch(str1, str1);
//	  KMPMatch match6 = new KMPMatch(str2, str1);
//	  KMPMatch match7 = new KMPMatch(str3, str1);
//	  KMPMatch match8 = new KMPMatch(str4, str1);
//	  
//	  System.out.println( match5.match() );
//	  System.out.println( match6.match() );
//	  System.out.println( match7.match() );
//	  System.out.println( match8.match() );
	  
  }
  
}