package unit;



public class StopWatch
{

  public long prefixRangeTime = 0;
  public long prefixAllTime = 0;
  public long substringRangeTime = 0;
  
  public long approximatePrefixRangeTime = 0;
  public long approximateSubstringRangeTime = 0;
  
  public long prefixSizeEstimationTime = 0;
  public long substringSizeEstimationTime = 0;      
  
  
  
  public int prefixRangeCount = 0;
  public int prefixAllCount = 0;
  public int substringRangeCount = 0;
  
  public int approximatePrefixRangeCount = 0;
  public int approximateSubstringRangeCount = 0;
  
  public int prefixSizeEstimationCount = 0;
  public int substringSizeEstimationCount = 0;
  
//  public TreeSet<Integer> set = new TreeSet<Integer> ();
  
  
  @Override
  public String toString()
  {
    String s = "";
    s +=  "type \t count" + " \t time \t avg \r\n";
    if ( prefixRangeCount != 0 ) {   
      s += "P \t " + prefixRangeCount + " \t " + prefixRangeTime + " \t "+ (prefixRangeTime / (prefixRangeCount * 1.0)) + "\r\n";
    } if ( prefixAllCount != 0 ) {
      s += "PR \t " + prefixAllCount + " \t " + prefixAllTime + " \t "+ (prefixAllTime/ (prefixAllCount * 1.0)) + "\r\n";
    } if ( substringRangeCount != 0 ) {
      s += "S \t " + substringRangeCount + " \t " + substringRangeTime + " \t "+ (substringRangeTime/ (substringRangeCount * 1.0)) + "\r\n";
    } if ( approximatePrefixRangeCount != 0 ) {
      s += "AP \t " + approximatePrefixRangeCount + " \t " + approximatePrefixRangeTime + " \t "+ (approximatePrefixRangeTime/ (approximatePrefixRangeCount * 1.0)) + "\r\n";
    } if ( approximateSubstringRangeCount != 0 ) {
      s += "AS \t " + approximateSubstringRangeCount + " \t " + approximateSubstringRangeTime + " \t "+ (approximateSubstringRangeTime/ (approximateSubstringRangeCount * 1.0)) + "\r\n";
    } if ( prefixSizeEstimationCount != 0 ) {
      s += "PSE \t " + prefixSizeEstimationCount + " \t " + prefixSizeEstimationTime + " \t "+ (prefixSizeEstimationTime/ (prefixSizeEstimationCount * 1.0)) + "\r\n";
    } if ( substringSizeEstimationCount != 0 ) {
      s += "SSE \t " + substringSizeEstimationCount + " \t " + substringSizeEstimationTime + " \t "+ (substringSizeEstimationTime/ (substringSizeEstimationCount * 1.0)) + "\r\n";
    }
    
    return s;
  }
    
    
}
