package com.bprise.orbit.migration.handler;

import java.util.ArrayList;
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
import com.datastax.driver.core.UDTValue;

import redis.clients.jedis.Jedis;

@Component
public class AdSpotHandler implements Handler {

	@Override
	public void update(List<Long> ids, Session session, Jedis jedis) {
		ids.forEach(p -> {
			jedis.keys("adspot_" + p + "*").forEach(t -> {
				jedis.del(t);
			});
			;
		});
		PreparedStatement prepared = session.prepare(QueryStore.GET_ADSPOT);
		BoundStatement bound = prepared.bind(ids);
		ResultSet rs = session.execute(bound);
		List<Row> rows = rs.all();
		for (Row row : rows) {
			createAdSpotRecord(row, jedis,session);
		}
		
	}

	private void createAdSpotRecord(Row row, Jedis jedis,Session session) {
		if (row.getByte("is_deleted") == 0) {
			Map<String, String> adSpotDetails = new HashMap<String, String>();
			adSpotDetails.put("is_mobile", String.valueOf(row.getByte("is_mobile")));
			adSpotDetails.put("is_rtb", String.valueOf(row.getByte("is_rtb")));
			adSpotDetails.put("name", row.getString("name"));
			adSpotDetails.put("vendorId",  String.valueOf(row.getLong("vendor_id")));
			adSpotDetails.put("pass_back_tag", null == row.getString("pass_back_tag")?"":row.getString("pass_back_tag"));
			adSpotDetails.put("vendorAppId", String.valueOf(row.getLong("vendor_app_id")));
			adSpotDetails.put("is_native", String.valueOf(row.getByte("is_native")));
			adSpotDetails.put("is_vast", String.valueOf(row.getByte("is_vast")));
			adSpotDetails.put("is_companion", String.valueOf(row.getByte("is_companion")));
			jedis.hmset("adspot_" + row.getLong("id"), adSpotDetails);
			jedis.set("adspotuid_"+row.getString("ad_spot_uid"),String.valueOf(row.getLong("id")));
			jedis.set("map_adspot_" + row.getLong("id") + ":vendor_" + row.getLong("vendor_id"), "");
			row.getSet("categoryids", Long.class).forEach(categoryids -> {
				jedis.sadd("category_spot_"+row.getLong("id"), String.valueOf(categoryids));
			});
//			jedis.set("map_adspot_" + row.getLong("id") + ":website_" + row.getLong("publisher_website"), "");
			/*PreparedStatement prepared = session.prepare(QueryStore.GET_PUBLISHER_WEBSITE);
			BoundStatement bound = prepared.bind(row.getLong("publisher_website"));
			ResultSet rs = session.execute(bound);
			List<Row> rows = rs.all();
			for (Row row1 : rows) {
				createPublisherWebsiteRecord(row1, jedis);
			}*/
			
			createBannerRecord(session,jedis,row);
			
			PreparedStatement prepared = session.prepare(QueryStore.GET_VENDORS);
			List<Long> vendorIds = new ArrayList<Long>();
			vendorIds.add(row.getLong("vendor_id"));
			BoundStatement bound = prepared.bind(vendorIds);
			ResultSet rs = session.execute(bound);
			Row vendorRow = rs.one();
			if ("CPM".equalsIgnoreCase(row.getString("revenue_type"))) {
				jedis.hset("budget_adspot_" + row.getLong("id"), "revenue_unit",
						String.valueOf(vendorRow.getDouble("cpm_rate")));
			}
			if ("CPC".equalsIgnoreCase(row.getString("revenue_type"))) {
				jedis.hset("budget_adspot_"+ row.getLong("id"), "revenue_unit",
						String.valueOf(vendorRow.getDouble("cpc_rate")));
			}
			jedis.hset("budget_adspot_"+ row.getLong("id"),"revenue_type",row.getString("revenue_type") == null ?"":row.getString("revenue_type"));
			
		}else{
			jedis.del("map_adspot_" + row.getLong("id") + ":vendor_" + row.getLong("vendor_id"));
			jedis.del("adspotuid_"+row.getString("ad_spot_uid"));
		}

	}
	
	private void createBannerRecord(Session session, Jedis jedis, Row row) {
		List<UDTValue> adSpotBannerList = row.getList("adspotbanner", UDTValue.class);
		adSpotBannerList.stream().forEach(p -> {
			jedis.del("banner_spot_"+row.getLong("id"));
			Map<String,String> bannerDetails = new HashMap<String,String>();
			bannerDetails.put("width",String.valueOf(p.getLong("width")));
			bannerDetails.put("height",String.valueOf(p.getLong("height")));
			bannerDetails.put("name",p.getString("name"));
			if(p.getString("name").contains("Mobile only")){
				bannerDetails.put("type", "Mobile");
			}
			if(p.getString("name").contains("Tablet only")){
				bannerDetails.put("type", "Tablet");
			}
			if(p.getString("name").contains("Mobile and Tablet")){
				bannerDetails.put("type", "Mobile/Tablet");
			}
			if(p.getString("name").contains("IAB")){
				bannerDetails.put("type", "Desktop");
			}
			jedis.hmset("banner_"+p.getLong("id"), bannerDetails);
			jedis.sadd("banner_spot_"+row.getLong("id"), String.valueOf(p.getLong("id")));
		});
	}
	
	/*private void createPublisherWebsiteRecord(Row row, Jedis jedis){
		if (row.getByte("is_deleted") == 0) {
			Map<String,String> webSiteDetails = new HashMap<String,String>();
			webSiteDetails.put("category_name",row.getString("category_name"));
			webSiteDetails.put("description",row.getString("description"));
			webSiteDetails.put("domain",row.getString("domain"));
			jedis.hmset("website_"+row.getLong("id"), webSiteDetails);
			jedis.set("map_vendor_"+row.getLong("vendor_id")+":website_"+row.getLong("id"), "");
		}else{
			jedis.del("map_vendor_"+row.getLong("vendor_id")+":website_"+row.getLong("id"));
			jedis.del("website_"+row.getLong("id"));
		}
		
	}*/
	
}
