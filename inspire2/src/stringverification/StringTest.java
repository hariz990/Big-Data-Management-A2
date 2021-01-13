package stringverification;

import java.util.TreeSet;

import unit.PositionalQgram;
import unit.QgramGenerator;

public class StringTest {

	
	public static void main(String[] args) 
	{
		QgramGenerator gen = new QgramGenerator();
		
		String queryString = "aaabaa";
		String objectString = "dddddddddaabacccccaaaccc";

		
		TreeSet<String> queryQgramSet = 
			gen.getQgramListWithoutWildCard(queryString);
		
		
		TreeSet<PositionalQgram> queryPositionalQgramSetWithoutWildCard =
			gen.getPositionalQgramListWithoutWildCard(queryString);
		
		TreeSet<PositionalQgram> objectPositionalQgram =
			gen.getPositionalQgramListWithoutWildCard(objectString);
		
		
		int gramLength = 2;
		int tau = 1;
		
		boolean isTauSubstring = StringVerification.isTauSubstring(
				queryString, 
				objectString,
				queryQgramSet,
				queryPositionalQgramSetWithoutWildCard, 
				objectPositionalQgram,
				gramLength,
				tau);
		
		System.out.println(isTauSubstring);
 	}

}
