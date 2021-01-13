package unit.serializer;

import unit.SpatialObject;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.serializers.FieldSerializer;

public class SpatialObjectSerializer extends FieldSerializer<SpatialObject>{

	public SpatialObjectSerializer(Kryo kryo) {
		super(kryo, SpatialObject.class);
		super.getField("_lat").setClass(Double.class);
		super.getField("_lng").setClass(Double.class);
		super.getField("_text").setClass(String.class);
	}
}
