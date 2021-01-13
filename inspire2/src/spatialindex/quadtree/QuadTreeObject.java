package spatialindex.quadtree;

import java.io.Serializable;

import spatialindex.spatialindex.Point;


public class QuadTreeObject implements Serializable {

	private static final long serialVersionUID = 1L;
	
	public int _objectid;
	public Point _point;

	/**
	 * Create a new NodeElement that holds the element at the given coordinates.
	 * 
	 * @param x
	 * @param y
	 * @param element
	 */
	public QuadTreeObject(Point point, int id) {
		this._point = point;
		this._objectid = id;
	}


	/**
	 * Returns the element that is contained within this NodeElement
	 * 
	 * @return
	 */
	public int getObjectID() {
		return _objectid;
	}

	
	/**
	 * Returns the point of the NodeElement
	 * 
	 * @return
	 */
	public Point getPoint() {
		return _point;
	}
}
