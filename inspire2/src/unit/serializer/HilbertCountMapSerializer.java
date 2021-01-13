package unit.serializer;

import java.util.TreeMap;

import unit.HilbertCountMap;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import com.esotericsoftware.kryo.serializers.MapSerializer;



public class HilbertCountMapSerializer extends FieldSerializer<HilbertCountMap>
{

	public HilbertCountMapSerializer(Kryo kryo) {
		super(kryo, HilbertCountMap.class);

		MapSerializer mapSerializer = new MapSerializer();
		mapSerializer.setKeyClass(String.class,  kryo.getSerializer(String.class));
		mapSerializer.setValueClass(Integer.class, kryo.getSerializer(Integer.class));
		mapSerializer.setKeysCanBeNull(false);
		mapSerializer.setValuesCanBeNull(false);
					
		super.getField("_map").setClass(TreeMap.class, mapSerializer);		
	}
}
