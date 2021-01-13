package sortmergejoin;

import java.util.ArrayList;
import java.util.TreeMap;
import java.util.TreeSet;

import unit.HilbertCountMap;

public class NodeLevelJoinTest
{

  /**
   * @param args
   */
  public static void main( String[] args )
  {
    // TODO Auto-generated method stub

    
    String str0 = "0";
    String str1 = "1";
    String str2 = "2";
    String str3 = "3";
    String str4 = "4";
    String str00 = "00";
    String str01 = "01";
    String str02 = "02";
    String str10 = "10";
    String str11 = "11";
    
    System.out.println( str0.compareTo( str1 ) );
    System.out.println( str1.compareTo( str2 ) );
    System.out.println( str2.compareTo( str3 ) );
  
    
    
     ArrayList<HilbertCountMap> vector = new ArrayList<HilbertCountMap> ();
    
     HilbertCountMap map1 = new HilbertCountMap();
     HilbertCountMap map2 = new HilbertCountMap();
     HilbertCountMap map3 = new HilbertCountMap();
     HilbertCountMap map4 = new HilbertCountMap();

    // NavigableMap<Integer, String> nm = new TreeMap<Integer, String>();

    // map1._map.put("1", 1);
    // map1._map.put("2", 2);
    // map1._map.put("3", 3);
    // map1._map.put("4", 4);
    //
    // map2._map.put("1", 1);
    // map2._map.put("2", 2);
    // map2._map.put("3", 3);
    // map2._map.put("4", 4);
    //
    // map3._map.put("1", 1);
    // map3._map.put("2", 2);
    // map3._map.put("3", 3);
    // map3._map.put("4", 4);
    //
    // map4._map.put("1", 1);
    // map4._map.put("2", 2);
    // map4._map.put("3", 3);
    // map4._map.put("4", 4);


//     map1._map.put("1", 1);
//     map1._map.put("5", 2);
//     map1._map.put("9", 3);
//     map1._map.put("13", 4);
//    
//     map2._map.put("2", 1);
//     map2._map.put("6", 2);
//     map2._map.put("10", 3);
//     map2._map.put("14", 4);
//    
//     map3._map.put("3", 1);
//     map3._map.put("7", 2);
//     map3._map.put("11", 3);
//     map3._map.put("15", 4);
//    
//     map4._map.put("4", 1);
//     map4._map.put("8", 2);
//     map4._map.put("12", 3);
//     map4._map.put("16", 4);


//     map1._map.put("7", 1);
//     map1._map.put("2", 2);
//     map1._map.put("4", 3);
//     map1._map.put("8", 4);
//    
//     map2._map.put("1", 1);
//     map2._map.put("3", 2);
//     map2._map.put("4", 3);
//     map2._map.put("7", 4);
//    
//     map3._map.put("1", 1);
//     map3._map.put("2", 2);
//     map3._map.put("4", 3);
//     map3._map.put("6", 4);
//    
//     map4._map.put("1", 1);
//     map4._map.put("3", 2);
//     map4._map.put("4", 3);
//     map4._map.put("5", 4);
    
     vector.add(map1);
     vector.add(map2);
     vector.add(map3);
     vector.add(map4);


//    ArrayList< TreeSet< String > > list = new ArrayList< TreeSet< String >>();
    

    NodeLevelJoin joinOperator = new NodeLevelJoin();
    TreeMap< String, ArrayList< Integer >> result = joinOperator.join( vector );

    System.out.println( result );

    System.out.println( map1 );
    System.out.println( map2 );
    System.out.println( map3 );
    System.out.println( map4 );

  }

}
