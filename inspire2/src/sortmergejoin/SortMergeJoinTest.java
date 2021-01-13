package sortmergejoin;

import java.util.ArrayList;
import java.util.NavigableSet;
import java.util.TreeSet;

public class SortMergeJoinTest {

	
	public static void main(String[] args) {
		
		ArrayList<NavigableSet<Integer>> array = new ArrayList<NavigableSet<Integer>>();
		
//		TreeSet<Integer> [] array = new TreeSet[4];
		
		TreeSet<Integer> a1 = new TreeSet<Integer>();
		a1.add(1);
		a1.add(5);
		a1.add(9);
		a1.add(13);
		
		TreeSet<Integer> a2 = new TreeSet<Integer>();
		a2.add(2);
		a2.add(6);
		a2.add(10);
		a2.add(14);
		
		TreeSet<Integer> a3 = new TreeSet<Integer>();
		a3.add(3);
		a3.add(7);
		a3.add(11);
		a3.add(15);
		
		TreeSet<Integer> a4 = new TreeSet<Integer>();
		a4.add(4);
		a4.add(8);
		a4.add(12);
		a4.add(16);
		
		array.add(a1);
		array.add(a2);
		array.add(a3);
		array.add(a4);
		
		SortMergeJoin joiner = new SortMergeJoin();
		System.out.println( joiner.join(array) );
		
		System.out.println( a1 );
		System.out.println( a2 );
		System.out.println( a3 );
		System.out.println( a4 );
		
		
		
	}

}
