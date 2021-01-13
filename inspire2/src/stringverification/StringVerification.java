package stringverification;

import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeSet;

import unit.PositionalQgram;

public class StringVerification {
	
	/**
	 * 
	 * @param str1
	 * @param str2
	 * @return str1 is a prefix to str2
	 */
	static public boolean isPrefix(String str1, String str2) {
		if (str1.length() > str2.length()) {
			return false;
		}
		
		if (str1.equals(str2.substring(0, str1.length()))) {
			return true;
		} else {
			return false;
		}
	}
	
	
	/**
	 * 
	 * @param str1
	 * @param str2
	 * @return str1 is a substring of str2
	 */
	static public boolean isSubstring(String str1, String str2) {
		
		if (str1.length() > str2.length()) {
			return false;
		}
		
		for (int i = 0; i <= str2.length() - str1.length(); i++) {
			String substring = str2.substring(i);
			if ( isPrefix(str1, substring )) {
				return true;
			}
		}
		return false;
	}
	
	/**
	 * 
	 * @param str1
	 * @param str2
	 * @param startPosition
	 * @return str1 is a substring of str2 for the startPosition
	 */
	static public boolean isSubstring(String str1, String str2, int startPosition) {
		if (str1.length() > str2.length()) {
			return false;
		}
		
		String substring = str2.substring(startPosition);
		if (isPrefix(str1, substring)) {
			return true;
		} else {
			return false;
		}
	}
	
	/**
	 * 
	 * @param str1
	 * @param str2
	 * @param startPositionSet
	 * @return str1 is a substring of str2 for one of the position in the startPositionSet
	 */
	static public boolean isSubstring(String str1, String str2, TreeSet<Integer> startPositionSet) {
		
		if (str1.length() > str2.length()) {
			return false;
		}
		
		Iterator<Integer> itr = startPositionSet.iterator();	
		while(itr.hasNext())
		{
			int position = itr.next();
			if ( position >= str2.length() ) {
				break;
			}
			
			String substring = str2.substring(position);
			if ( isPrefix(str1, substring) ) {
				return true;
			}
		}
		return false;
	}
	
	
	static public int getEditDistance(String str1, String str2){
		// distance matrix
		int[][] distance = new int[str1.length() + 1][str2.length() + 1];
		for (int i = 0; i <= str1.length(); i++){
			// the distance of any first string to an empty second string
			distance[i][0] = i;
		}
		for (int j = 0; j <= str2.length(); j++){
			// the distance of any second string to an empty first string
			distance[0][j] = j;
		}
		// calculate edit distance
		for (int i = 1; i <= str1.length(); i++){
			for (int j = 1; j <= str2.length(); j++){
				if(str1.substring(i - 1, i).equalsIgnoreCase(str2.substring(j - 1, j)) ){
					distance[i][j] = distance[i-1][j-1];
				}
				else{
					distance[i][j] = Math
							.min(Math.min(distance[i - 1][j] + 1,
									distance[i][j - 1] + 1),
									distance[i - 1][j - 1] + 1);
				}
			}
		}
		return distance[str1.length()][str2.length()];
	}
	
	
	static public boolean isWithinEditDistance(String str1, String str2, int tau)
	{
		// test on length
		if (Math.abs(str1.length() - str2.length()) > tau){
			return false;
		}
		// initialize the first row for empty str1, until tau
		int[][] distance = new int[2][str1.length() + 1];
		for (int j = 0; j <= Math.min(str1.length(), tau); j++){
			distance[0][j] = j;
		}	
		distance[1][str1.length()] = tau + 1;
		
		// start from the second row
		for (int i = 2; i <= str2.length() + 1; i++) {
			
			int m = tau + 1;
			
//			for (int j = Math.max(1, Math.min(1, i - tau)); j <= Math.min(str1.length() + 1, i + tau); j++){
			for (int j = Math.max(1, i - tau); j <= Math.min(str1.length() + 1, i + tau); j++){
				
				// str1 append
				int d1 = (j < i + tau) ? (distance[0][j - 1] + 1) : (tau + 1);
				// str2 append
				int d2 = (j > 1) ? (distance[1][j - 2] + 1) : (tau + 1);		
				// substitute
				int d3 = (j > 1) ? (distance[0][j - 2] + 
						(str2.substring(i - 2, i - 1).equalsIgnoreCase(str1.substring(j - 2, j - 1)) ? 0 : 1))
						: (tau + 1);
				
				distance[1][j - 1] = Math.min(Math.min(d1, d2), d3);
				m = Math.min(m, distance[1][j - 1]);
			}
			
			if (m > tau)
			{
				return false;
			}

			// for next round
			for (int j = Math.max(1, i - tau); j <= Math.min(str1.length() + 1, i + tau); j++){
				distance[0][j-1] = distance[1][j-1];
			}
		}
		return (distance[1][str1.length()] <= tau);
	}
	
	/**
	 * 
	 * @param str1
	 * @param str2
	 * @param tau
	 * @return whether str1 is a tau-prefix of str2
	 */
	
	static public boolean isTauPrefix(String str1, String str2, int tau)
	{
	    if( str1.length() <= tau ) {
	        return true; 
	    }
		// if the length of str1 is greater than str2 + tau, return false
	    else if (str1.length() - str2.length() > tau){
			return false;
		}
		
		// initialize the first row for empty str1, until tau
		int[][] distance = new int[2][str1.length() + 1];
		for (int j = 0; j <= Math.min(str1.length(), tau); j++){
			distance[0][j] = j;
		}	
		distance[1][str1.length()] = tau + 1;
		
		// start from the second row
		for (int i = 2; i <= str2.length() + 1; i++)
		{
			int m = tau + 1;
			
//			for (int j = Math.max(1, Math.min(1, i - tau)); j <= Math.min(str1.length() + 1, i + tau); j++){
			for (int j = Math.max(1, i - tau); j <= Math.min(str1.length() + 1, i + tau); j++){
				
				// str1 append
				int d1 = (j < i + tau) ? (distance[0][j - 1] + 1) : (tau + 1);
				// str2 append
				int d2 = (j > 1) ? (distance[1][j - 2] + 1) : (tau + 1);		
				// substitute
				int d3 = (j > 1) ? (distance[0][j - 2] + 
						(str2.substring(i - 2, i - 1).equalsIgnoreCase(str1.substring(j - 2, j - 1)) ? 0 : 1))
						: (tau + 1);
				
				distance[1][j - 1] = Math.min(Math.min(d1, d2), d3);
				m = Math.min(m, distance[1][j - 1]);
			}
			if (m > tau)
			{
				return false;
			}
			
			if (distance[1][str1.length()] <= tau) 
			{
				return true;
			}
			
			// for next round
			for (int j = Math.max(1, i - tau); j <= Math.min(str1.length() + 1, i + tau); j++){
				distance[0][j-1] = distance[1][j-1];
			}
		}
		return (distance[1][str1.length()] <= tau);
	}
	
	
	static public boolean isTauPrefix(String str1, String str2, 
			Hashtable<String, BitSet> queryHashtable, int gramLength, int tau){
		
		// if the length of str1 is greater than str2 + tau, return false
		if (str1.length() - str2.length() > tau){
			return false;
		}
		
		/*
		int missCount = 0; 
		int maxMissMatch = (gramLength * tau);
		
		StringGramList gen = new StringGramList();
		Hashtable<String, BitSet> objHashtable = gen.getGramPosBitSetHashtable(str2);		
		
		if (str1.length() - gramLength + 1 > maxMissMatch) {
			Iterator<Entry<String, BitSet>> queryItr = queryHashtable.entrySet().iterator();
			while (queryItr.hasNext()) {
				Entry<String, BitSet> queryEntry = queryItr.next();
				String queryGram = queryEntry.getKey();
				BitSet queryPositionBitSet = queryEntry.getValue();
				
				BitSet objectPositionBitSet = objHashtable.get(queryGram);
				if (objectPositionBitSet == null) {
 					missCount += queryPositionBitSet.cardinality();
					if (missCount > maxMissMatch) {
						return false;
					}
				} else {
					if (queryPositionBitSet.cardinality() > objectPositionBitSet.cardinality()) {
						missCount += queryPositionBitSet.cardinality() - objectPositionBitSet.cardinality();
						if (missCount > maxMissMatch) {
							return false;
						}
					} 
				}			
			}
		}
	
		
		int matchCount = 0;
		int minMatch = (str1.length() - gramLength + 1) - (gramLength * tau);
			
		Iterator<Entry<String, BitSet>> queryItr = queryHashtable.entrySet().iterator();
		while (queryItr.hasNext()) {
			Entry<String, BitSet> queryEntry = queryItr.next();
			String queryGram = queryEntry.getKey();
			BitSet queryPositionBitSet = queryEntry.getValue();
			
			BitSet objPositionBitSet = objHashtable.get(queryGram);
			if (objPositionBitSet != null) {				
				BitSet objJoinBitSet = new BitSet();				
				for (int pos = objPositionBitSet.nextSetBit(0); pos >= 0; pos = objPositionBitSet.nextSetBit(pos + 1)) {	
					for (int joinBit = pos - tau ; joinBit <= pos + tau ; joinBit++) {					
						if (joinBit >= 0) {
							objJoinBitSet.set(joinBit);
						}
					}
				}
				// intersect two bitsets
				objJoinBitSet.and(queryPositionBitSet);
				// modify the matching gram count for the child
				matchCount += Math.min(objJoinBitSet.cardinality(), objPositionBitSet.cardinality());
			}			
		}
		
		if (matchCount < minMatch) {
			return false;
		}
		*/
		
		
		
		// initialize the first row for empty str1, until tau
		int[][] distance = new int[2][str1.length() + 1];
		for (int j = 0; j <= Math.min(str1.length(), tau); j++){
			distance[0][j] = j;
		}	
		distance[1][str1.length()] = tau + 1;
		
		// start from the second row
		for (int i = 2; i <= str2.length() + 1; i++) {
			
			int m = tau + 1;
			
			for (int j = Math.max(1, i - tau); j <= Math.min(str1.length() + 1, i + tau); j++){
				
				// str1 append
				int d1 = (j < i + tau) ? (distance[0][j - 1] + 1) : (tau + 1);
				// str2 append
				int d2 = (j > 1) ? (distance[1][j - 2] + 1) : (tau + 1);		
				// substitute
				int d3 = (j > 1) ? (distance[0][j - 2] + 
						(str2.substring(i - 2, i - 1).equalsIgnoreCase(str1.substring(j - 2, j - 1)) ? 0 : 1))
						: (tau + 1);
				
				distance[1][j - 1] = Math.min(Math.min(d1, d2), d3);
				m = Math.min(m, distance[1][j - 1]);
			}
			if (m > tau)
			{
				return false;
			}
			
			if (distance[1][str1.length()] <= tau) {
				return true;
			}
			
			
			for (int j = Math.max(1, i - tau); j <= Math.min(str1.length() + 1, i + tau); j++){
				distance[0][j-1] = distance[1][j-1];
			}
			
		}
		return (distance[1][str1.length()] <= tau);
	}
	
	
	// naive method.
	static public boolean isTauSubstring(String str1, String str2, int tau){	
	
		boolean flag = false;
		for(int i = 0; i < str2.length(); i++) {
			String substring = str2.substring(i);
			if ( isTauPrefix(str1, substring, tau) ) {
				flag = true;
				break;
			}
		}
		return flag;
	}
	
	
	
	static public boolean passTauSubstringCountFilter(
			final TreeSet<PositionalQgram> queryGramSet,
			final TreeSet<PositionalQgram> objectGramSet,
			final int gramLength,
			final int tau)
	{
		int minMatchThreshold = queryGramSet.size() - gramLength * tau; 
		
		Iterator<PositionalQgram> queryItr = queryGramSet.iterator();
		Iterator<PositionalQgram> objectItr = objectGramSet.iterator();
		
		PositionalQgram objectQgram = null;
		PositionalQgram queryQgram = null;
		int comparedValue = 0;
		int count = 0;
		
		// check the number of matching q-grams 
		// consider the token only
		while ( queryItr.hasNext() || objectItr.hasNext() )
		{
			if ( comparedValue == 0 )
			{
				if ( queryItr.hasNext() && objectItr.hasNext() ) {
					queryQgram = queryItr.next();
					objectQgram = objectItr.next();					
				} else {
					break;
				}
			}
			// query gram iterator move
			else if ( comparedValue < 0 )
			{
				if ( queryItr.hasNext() ) {
					queryQgram = queryItr.next();
				}  else {
					break;
				}
			}
			// object gram iterator move
			else
			{
				if ( objectItr.hasNext() ) {
					objectQgram = objectItr.next();
				} else {
					break;
				}
			}
			
			// only need to compare the token
			comparedValue = queryQgram._qgram.compareTo(objectQgram._qgram);			
			if ( comparedValue == 0 )				
			{				
				count ++;				
			}
			// else, skip
		}
		
		if ( count >= minMatchThreshold )
		{
			return true;
		}
		else
		{
			return false;
		}
	}
	
	
	
	static public TreeSet<Integer> getTauSubstringStartPosition(
			final TreeSet<String> queryGramSet,
			final TreeSet<PositionalQgram> queryPosGramSet,
			final TreeSet<PositionalQgram> objectPosGramSet,
			final int gramLength,
			final int tau)
	{
		TreeSet<Integer> result = new TreeSet<Integer>();
		HashMap<Integer, Integer> startMap = new HashMap<Integer, Integer>();
		
		int minMatchThreshold = queryPosGramSet.size() - gramLength * tau; 
		
		Iterator<String> queryGramItr = queryGramSet.iterator();
		Iterator<PositionalQgram> queryPosQgramItr = queryPosGramSet.iterator();
		Iterator<PositionalQgram> objectPosQgramItr = objectPosGramSet.iterator();
		
		String gramToken = null;		
		PositionalQgram queryPosQgram = null;
		PositionalQgram objectPosQgram = null;
				
		
		while ( queryGramItr.hasNext() )
		{
			gramToken = queryGramItr.next();
			
			// get the relative positions for the query q-gram
			TreeSet<Integer> relativePositions = new TreeSet<Integer>();
			
			// if it is the first
			if ( queryPosQgram == null )
			{
				queryPosQgram = queryPosQgramItr.next();
			}
			
			// if have the same q-gram token
			while ( queryPosQgram._qgram.equals( gramToken ) )
			{
				relativePositions.add( queryPosQgram._pos );
				
				// get the next q-gram
				if ( queryPosQgramItr.hasNext() )
				{
					queryPosQgram = queryPosQgramItr.next();
				}
				else
				{
					break;
				}
			}
			
			TreeSet<Integer> objectGramPositions = new TreeSet<Integer>();
			
			// first gram
			if ( objectPosQgram == null )
			{
				objectPosQgram = objectPosQgramItr.next();
			}
			
			// get the object gram positions
			int objectComparedValue = objectPosQgram._qgram.compareTo( gramToken ); 
			while ( objectComparedValue <= 0 )
			{				
				// if have the same token
				if ( objectComparedValue == 0 )
				{
					objectGramPositions.add( objectPosQgram._pos );
				}
				
				// get the next q-gram
				if ( objectPosQgramItr.hasNext() )
				{
					objectPosQgram = objectPosQgramItr.next();
					objectComparedValue = objectPosQgram._qgram.compareTo( gramToken );
				}
				else
				{
					break;
				}
			}
			
			if ( ! relativePositions.isEmpty() && ! objectGramPositions.isEmpty() )
			{
//				int maxMatch = Math.max( relativePositions.size(), objectGramPositions.size() );
				int maxMatch = Math.min( relativePositions.size(), objectGramPositions.size() );
				
				HashMap<Integer, Integer> tempStartMap = new HashMap<Integer, Integer>();
				
				for ( int relativePosition : relativePositions )
				{					
					HashSet<Integer> startSet = new HashSet<Integer>();
					
					for ( int objectPosition : objectGramPositions )
					{
						int start = objectPosition - relativePosition;						
						// get the matching position
						for ( int i = Math.max( 0, start - tau ); i <= start + tau; i++ )
						{
							startSet.add(i);
						}
					}					
					for ( int start : startSet )
					{
						Integer count = tempStartMap.get( start );
						if ( count == null )
						{
							tempStartMap.put( start, 1 );
						}
						else
						{
							count += 1;
							tempStartMap.put( start, Math.min(maxMatch, count) );
						}
					}
				}
				
				Iterator<Entry<Integer, Integer>> tempItr = tempStartMap.entrySet().iterator();
				while ( tempItr.hasNext() )
				{
					Entry<Integer, Integer> tempEntry = tempItr.next();
					int start = tempEntry.getKey();
					int tempCount = tempEntry.getValue();
					
					Integer fullCount = startMap.get( start );
					if ( fullCount == null )
					{
						startMap.put( start, tempCount );
					}
					else 
					{
						startMap.put( start, fullCount + tempCount );
					}
				}	
				tempStartMap.clear();
			}			
		}
		
		Iterator<Entry<Integer, Integer>> itr = startMap.entrySet().iterator();
		while ( itr.hasNext() )
		{
			Entry<Integer, Integer> entry = itr.next();
			int start = entry.getKey();
			int count = entry.getValue();
			
			if ( count >= minMatchThreshold )
			{
				result.add( start );
			}
		}
		
		startMap.clear();		
		return result.isEmpty() ? null : result;		
	}
	
	static public boolean isTauSubstring(
			final String queryString, 
			final String objectString, 
			final TreeSet<String> queryGramSet,
			final TreeSet<PositionalQgram> queryPosGramSet,
			final TreeSet<PositionalQgram> objectPosGramSet,
			final int gramLength,
			final int tau	
			)
	{
		// check the length
		if ( queryString.length() - objectString.length() > tau )
		{
			return false;
		}
		
		if ( passTauSubstringCountFilter( queryPosGramSet, objectPosGramSet, gramLength, tau ) )
		{			
			TreeSet<Integer> startPositions = 
				getTauSubstringStartPosition( 
						queryGramSet,
						queryPosGramSet,
						objectPosGramSet,
						gramLength,
						tau );
			
			if ( startPositions != null )
			{
				for ( int start : startPositions )
				{
					String substring = objectString.substring(start);
					if ( isTauPrefix( queryString, substring, tau ) ) {						
						return true;
					}
				}
			}			
		}
		
		return false;
	}
	
	
	static public boolean isTauSubstring(
			final String queryString, 
			final String objectString, 
			final int tau,
			final TreeSet<Integer> startPositions )
	{
		// check the length
		if ( queryString.length() - objectString.length() > tau )
		{
			return false;
		}
					
		for ( int start : startPositions )
		{
			String substring = objectString.substring(start);
			if ( isTauPrefix( queryString, substring, tau ) ) {						
				return true;
			}
		}
							
		return false;
	}
	
	
	
	
	
//	static public boolean isTauSubstring(
//			String qStr, String objStr, 
//			Hashtable<String, BitSet> queryHashtable, 
//			int gramLength, int tau, TreeSet<Integer> startSetCopy )
//	{	
//		TreeSet<Integer> startSet = (TreeSet<Integer>) startSetCopy.clone();
//		if (startSet.size() > 10) {
//			int missCount = 0; 
//			int maxMissMatch = (gramLength * tau);
//			
//			QgramGenerator gen = new QgramGenerator();
//			Hashtable<String, BitSet> objHashtable = gen.getGramPosBitSetHashtable(objStr);		
//			
//			if (qStr.length() - gramLength + 1 > maxMissMatch) {
//				Iterator<Entry<String, BitSet>> queryItr = queryHashtable.entrySet().iterator();
//				while (queryItr.hasNext()) {
//					Entry<String, BitSet> queryEntry = queryItr.next();
//					String queryGram = queryEntry.getKey();
//					BitSet queryPositionBitSet = queryEntry.getValue();
//					
//					BitSet objectPositionBitSet = objHashtable.get(queryGram);
//					if (objectPositionBitSet == null) {
//	 					missCount += queryPositionBitSet.cardinality();
//						if (missCount > maxMissMatch) {
//							return false;
//						}
//					} else {
//						if (queryPositionBitSet.cardinality() > objectPositionBitSet.cardinality()) {
//							missCount += queryPositionBitSet.cardinality() - objectPositionBitSet.cardinality();
//							if (missCount > maxMissMatch) {
//								return false;
//							}
//						} 
//					}			
//				}
//			}
//		
//			int minMatch = (qStr.length() - gramLength + 1) - (gramLength * tau);
//			
//			Iterator<Integer> startItr = startSet.iterator();
//			while(startItr.hasNext())
//			{
//				int start = startItr.next();
//				int matchCount = 0;
//				
//				Iterator<Entry<String, BitSet>> queryItr = queryHashtable.entrySet().iterator();
//				while (queryItr.hasNext()) {
//					Entry<String, BitSet> queryEntry = queryItr.next();
//					String queryGram = queryEntry.getKey();
//					BitSet queryPositionBitSet = queryEntry.getValue();
//					
//					BitSet objPositionBitSet = objHashtable.get(queryGram);
//					if (objPositionBitSet != null) {				
//						BitSet objJoinBitSet = new BitSet();				
//						for (int pos = objPositionBitSet.nextSetBit(0); pos >= 0; pos = objPositionBitSet.nextSetBit(pos + 1)) {	
//							for (int joinBit = pos - tau - start; joinBit <= pos + tau - start; joinBit++) {					
//								if (joinBit >= 0) {
//									objJoinBitSet.set(joinBit);
//								}
//							}
//						}
//						// intersect two bitsets
//						objJoinBitSet.and(queryPositionBitSet);
//						// modify the matching gram count for the child
//						matchCount += Math.min(objJoinBitSet.cardinality(), objPositionBitSet.cardinality());
//					}			
//				}
//				
//				if (matchCount < minMatch) {
//					startItr.remove();
//				}				
//			}	
//		}		
//		
//		boolean flag = false;
//		Iterator<Integer> startItr = startSet.iterator();
//		while(startItr.hasNext()) {
//			int start = startItr.next();
//			if (start < objStr.length()) {
//				String substring = objStr.substring(start);
//				if (isTauPrefix(qStr, substring, tau)) {
//					flag = true;
//					break;
//				}
//			}
//		}
//		
//		return flag;
//	}
//	
//	static public boolean gramNumberFilter(String qText, String objText, int gramLength, int tau) {
//		QgramGenerator gen = new QgramGenerator();
//		Hashtable<String, TreeSet<Integer>> qHashtable = gen.getGramPosHashtable(qText);
//		Hashtable<String, TreeSet<Integer>> objHashtable = gen.getGramPosHashtable(objText);
//				
//		int minMatch = (qText.length() - gramLength + 1) - (gramLength * tau);
//		int maxMissMatch = (gramLength * tau); 
//					
//		return gramNumberFilter(qHashtable, objHashtable, minMatch, maxMissMatch);
//	}
//	
//	static public boolean gramNumberFilter(
//			final Hashtable<String, TreeSet<Integer>> queryHashtable, 
//			final Hashtable<String, TreeSet<Integer>> objHashtable,
//			final int minMatch,
//			final int maxMissMatch) {
//		
//		int missMatchCount = 0;
//		int matchCount = 0;
//		Iterator<Entry<String, TreeSet<Integer>>> queryItr = queryHashtable.entrySet().iterator();
//		while (queryItr.hasNext()) {
//			Entry<String, TreeSet<Integer>> queryEntry = queryItr.next();
//			String gram = queryEntry.getKey();
//			TreeSet<Integer> queryPositionSet = queryEntry.getValue();
//			
//			TreeSet<Integer> objPositionSet = objHashtable.get(gram);
//			if (objPositionSet == null) {
//				missMatchCount += queryPositionSet.size();
//			} else {
//				
//				missMatchCount += (queryPositionSet.size() >= objPositionSet.size()) ? 0
//						: (queryPositionSet.size() - objPositionSet.size());
//				
//				matchCount += (queryPositionSet.size() >= objPositionSet.size()) ? queryPositionSet.size()
//						: objPositionSet.size();
//			}
//			if (missMatchCount >= maxMissMatch) {
//				return false;
//			}
//			if (matchCount >= minMatch) {
//				return true;
//			}
//		}
//		return (matchCount >= minMatch);
//	}
	
	
}
