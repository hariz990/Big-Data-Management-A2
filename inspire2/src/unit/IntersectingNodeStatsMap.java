package unit;

import java.util.TreeMap;

import unit.comparator.HilbertComparator;


public class IntersectingNodeStatsMap extends TreeMap<String, NodeStatistic>
{
  private static final long serialVersionUID = 1L;
    
  public int denseNodeNumber = 0;
  
  public IntersectingNodeStatsMap( )
  {
    super( new HilbertComparator() );
  }
  
  
  @Override
  public String toString()
  {
    return super.toString();
  }
}
