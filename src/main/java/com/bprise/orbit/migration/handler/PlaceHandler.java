package com.bprise.orbit.migration.handler;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Component;

import com.bprise.orbit.migration.util.QueryStore;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import redis.clients.jedis.Jedis;

@Component
public class PlaceHandler implements Handler {

	@Override
	public void update(List<Long> ids, Session session, Jedis jedis) {
		ids.forEach(p -> {
			jedis.keys("place_" + p + "*").forEach(t -> {
				jedis.del(t);
			});
			;
		});
		PreparedStatement prepared = session.prepare(QueryStore.GET_PLACE);
		BoundStatement bound = prepared.bind(ids);
		ResultSet rs = session.execute(bound);
		List<Row> rows = rs.all();
		for (Row row : rows) {
			createPlaceRecord(row, jedis);
		}

	}
	
	
	public void createPlaceRecord(Row row,Jedis jedis){
		Map<String,String> placeDetails = new HashMap<String,String>();
		placeDetails.put("place_name", row.getString("place_name"));
		placeDetails.put("vendor_id",  String.valueOf(row.getLong("vendor_id")));
		placeDetails.put("Timezone", row.getString("zonalname"));
		placeDetails.put("googleUid", row.getString("google_uid"));
		placeDetails.put("latitude", String.valueOf(row.getDouble("latitude")));
		placeDetails.put("longitude",String.valueOf(row.getDouble("longitude")));
		jedis.set("place_"+row.getLong("id")+":"+row.getString("place_uid"),"");
		jedis.hmset("place_"+row.getLong("id"), placeDetails);
	}

}
