package unit.comparator;

import java.util.Comparator;

import spatialindex.quadtree.QuadTreeNode;
import spatialindex.spatialindex.Region;

public class HilbertComparatorByDistance implements Comparator<String> {

	private QuadTreeNode _root;
	private Region _queryRegion;
	
	public HilbertComparatorByDistance( QuadTreeNode root, Region queryRegion )
	{
		_root = root;
		_queryRegion = queryRegion; 
	}
	
	@Override
	public int compare(String hilbertCode1, String hilbertCode2) {
		
		Region region1 = _root.getLeafNode(hilbertCode1)._region;
		Region region2 = _root.getLeafNode(hilbertCode2)._region;
		
		double dist1 = _queryRegion.getMinimumDistance(region1);
		double dist2 = _queryRegion.getMinimumDistance(region2);
		
		return Double.compare(dist1, dist2);
	}

}
