package com.bprise.orbit.migration.handler;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;

import com.bprise.orbit.migration.util.QueryStore;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import redis.clients.jedis.Jedis;

@Component
public class MediumHandler implements Handler {

	@Override
	public void update(List<Long> ids, Session session, Jedis jedis) {
		ids.forEach(p -> {
			jedis.keys("medium_" + p + "*").forEach(t -> {
				jedis.del(t);
			});
			;
		});
		PreparedStatement prepared = session.prepare(QueryStore.GET_MEDIUM);
		BoundStatement bound = prepared.bind(ids);
		ResultSet rs = session.execute(bound);
		List<Row> rows = rs.all();
		for (Row row : rows) {
			// createMediumRecord(row, jedis);
			createMediumRecordList(row, jedis, session);
		}
	}

	/*
	 * private void createMediumRecord(Row row,Jedis jedis){ Map<String,String>
	 * mediumDetails = new HashMap<String,String>(); mediumDetails.put("name",
	 * row.getString("name")); mediumDetails.put("campaign_id",
	 * String.valueOf(row.getLong("campaign_id")));
	 * jedis.hmset("medium_"+row.getLong("id"), mediumDetails);
	 * jedis.set("map_medium_"+row.getLong("id")+":camp_"+row.getLong(
	 * "campaign_id"), ""); }
	 */
	private void createMediumRecordList(Row row, Jedis jedis, Session session) {
		if ("Own App".equalsIgnoreCase(row.getString("name"))) {
			if ("Running".equalsIgnoreCase(row.getString("status"))) {
				jedis.sadd(
						row.getString("name").replaceAll(" ", "").toLowerCase() + "_"
								+ String.valueOf((row.getLong("vendor_id"))),
						"camp_" + String.valueOf((row.getLong("campaign_id"))));
				jedis.hset("camp_" + row.getLong("campaign_id"), "is_fb",
						String.valueOf(row.getByte("to_be_used_in_fb")));

			} else {
				jedis.srem(
						row.getString("name").replaceAll(" ", "").toLowerCase() + "_"
								+ String.valueOf((row.getLong("vendor_id"))),
						"camp_" + String.valueOf((row.getLong("campaign_id"))));
			}
		} else {
			if ("Running".equalsIgnoreCase(row.getString("status"))) {
				jedis.sadd(row.getString("name").replaceAll(" ", "").toLowerCase(),
						"camp_" + String.valueOf((row.getLong("campaign_id"))));
				jedis.hset("camp_" + row.getLong("campaign_id"), "is_fb",
						String.valueOf(row.getByte("to_be_used_in_fb")));
				jedis.sadd("serve_"+row.getString("name").replaceAll(" ", "").toLowerCase()+"_camp",
						"camp_" + String.valueOf((row.getLong("campaign_id"))));
				setBudgetKeys(row, jedis, session);
				setBudgetCalculation(row, jedis);
				
				/*setDailyBudgetAndCalculation(row, jedis);*/

			} else {
				jedis.srem(row.getString("name").replaceAll(" ", "").toLowerCase(),
						"camp_" + String.valueOf((row.getLong("campaign_id"))));
				jedis.srem("serve_"+row.getString("name").replaceAll(" ", "").toLowerCase()+"_camp",
						"camp_" + String.valueOf((row.getLong("campaign_id"))));
			}
		}
		// Map<String, String> budgetDetails = new HashMap<String, String>();
		// if(!jedis.exists("budget_"+row.getString("name").replaceAll(" ",
		// "")+"_camp_"+row.getLong(Constants.CAMPAIGN_CASSANDRA_ID))){
		// budgetDetails.put("budget_left",
		// String.valueOf(row.getDouble("budget")));
		// budgetDetails.put("reference", "1");
		// budgetDetails.put("daily_budget_left",
		// String.valueOf(row.getDouble("budget")));
		// jedis.hmset("budget_"+row.getString("name").replaceAll(" ",
		// "")+"camp_"+row.getLong(Constants.CAMPAIGN_CASSANDRA_ID),budgetDetails);
		// }

	}

	private void setBudgetKeys(Row row, Jedis jedis, Session session) {
		if ("LIFETIME".equalsIgnoreCase(row.getString("budget_type"))) {
			jedis.hset("budget_" + row.getString("name").replaceAll(" ", "").toLowerCase() + "_camp_" + row.getLong("campaign_id"),
					"is_daily_campaign", "0");
		} else if ("DAILY".equalsIgnoreCase(row.getString("budget_type"))){
			jedis.hset("budget_" + row.getString("name").replaceAll(" ", "").toLowerCase() + "_camp_" + row.getLong("campaign_id"),
					"is_daily_campaign", "1");
		}
		jedis.hset("budget_" + row.getString("name").replaceAll(" ", "").toLowerCase() + "_camp_" + row.getLong("campaign_id"), "revenue_type",
				row.getString("revenue_type") == null? "":row.getString("revenue_type"));
		PreparedStatement prepared = session.prepare(QueryStore.GET_VENDORS);
		List<Long> vendorIds = new ArrayList<Long>();
		vendorIds.add(row.getLong("vendor_id"));
		BoundStatement bound = prepared.bind(vendorIds);
		ResultSet rs = session.execute(bound);
		Row vendorRow = rs.one();
		if ("CPM".equalsIgnoreCase(row.getString("revenue_type"))) {
			jedis.hset("budget_" + row.getString("name").replaceAll(" ", "").toLowerCase() + "_camp_" + row.getLong("campaign_id"), "revenue_unit",
					String.valueOf(vendorRow.getDouble("cpm_rate"))== null ?"":String.valueOf(vendorRow.getDouble("cpm_rate")));
		}
		if ("CPC".equalsIgnoreCase(row.getString("revenue_type"))) {
			jedis.hset("budget_" + row.getString("name").replaceAll(" ", "").toLowerCase() + "_camp_" + row.getLong("campaign_id"), "revenue_unit",
					String.valueOf(vendorRow.getDouble("cpc_rate"))== null ?"":String.valueOf(vendorRow.getDouble("cpc_rate")));
		}
		jedis.hset("camp_" + row.getLong("campaign_id"), "commission",
				String.valueOf(vendorRow.getDouble("commission")) == null ?"":String.valueOf(vendorRow.getDouble("commission")));
	}

	/*private void setDailyBudgetAndCalculation(Row row, Jedis jedis) {
		String dailyBudgetLeft = jedis.hget(
				"budget_" + row.getString("name") + "camp_" + row.getLong("campaign_id"), "daily_budget_left");
		if (dailyBudgetLeft != null) {
			double budgetDiff = row.getDouble("budget") - (Double.valueOf(dailyBudgetLeft));
			if (budgetDiff > 0) {
				String dailyBudgetCalc = jedis.hget(
						"budget_" + row.getString("name") + "camp_" + row.getLong("campaign_id"),
						"daily_budget_calc");
				if (dailyBudgetCalc != null) {
					double finalDailyBudgetLeft = budgetDiff + Double.valueOf(dailyBudgetCalc);
					jedis.hset("budget_" + row.getString("name") + "camp_" + row.getLong("campaign_id"),
							"daily_budget_left", String.valueOf(finalDailyBudgetLeft));
					jedis.hset("budget_" + row.getString("name") + "camp_" + row.getLong("campaign_id"),
							"reference", "0");
				}
			}
		} else {
			jedis.hset("budget_" + row.getString("name").replaceAll(" ", "").toLowerCase() + "camp_" + row.getLong("campaign_id"),
					"daily_budget_left", String.valueOf(row.getDouble("budget")));
			jedis.hset("budget_" + row.getString("name").replaceAll(" ", "").toLowerCase() + "camp_" + row.getLong("campaign_id"), "reference",
					"0");
		}
	}*/

	private void setBudgetCalculation(Row row, Jedis jedis) {
		String budget = jedis.hget("budget_" + row.getString("name").replaceAll(" ", "").toLowerCase() + "_camp_" + row.getLong("campaign_id"),
				"budget");
		String reference = jedis.hget("budget_" + row.getString("name").replaceAll(" ", "").toLowerCase() + "_camp_" + row.getLong("campaign_id"),
				"reference");
		if (budget != null) {
			double budgetDiff = row.getDouble("budget") - (Double.valueOf(budget));
			if (budgetDiff > 0 && reference.equals("1") ) {
				String budgetCalc = jedis.hget(
						"budget_" + row.getString("name").replaceAll(" ", "").toLowerCase() + "_camp_" + row.getLong("campaign_id"),
						"budget_calc");
				if (budgetCalc != null) {
					double finalBudgetLeft = budgetDiff + Double.valueOf(budgetCalc);
					jedis.hset("budget_" + row.getString("name").replaceAll(" ", "").toLowerCase() + "_camp_" + row.getLong("campaign_id"),
							"budget_left", String.valueOf(finalBudgetLeft));
					jedis.hset("budget_" + row.getString("name").replaceAll(" ", "").toLowerCase() + "_camp_" + row.getLong("campaign_id"),
							"reference", "0");
					jedis.hset("budget_" + row.getString("name").replaceAll(" ", "").toLowerCase() + "_camp_" + row.getLong("campaign_id"),
							"budget", String.valueOf(row.getDouble("budget")));
				}
			}else if(budgetDiff > 0 && reference.equals("0") ){
				String budgetLeft = jedis.hget("budget_" + row.getString("name").replaceAll(" ", "").toLowerCase() + "_camp_" + row.getLong("campaign_id"),
						"budget_left");
				if(budgetLeft != null){
				double finalBudgetLeft = budgetDiff + Double.valueOf(budgetLeft);
				jedis.hset("budget_" + row.getString("name").replaceAll(" ", "").toLowerCase() + "_camp_" + row.getLong("campaign_id"),
						"budget_left", String.valueOf(finalBudgetLeft));
				jedis.hset("budget_" + row.getString("name").replaceAll(" ", "").toLowerCase() + "_camp_" + row.getLong("campaign_id"),
						"reference", "0");
				jedis.hset("budget_" + row.getString("name").replaceAll(" ", "").toLowerCase() + "_camp_" + row.getLong("campaign_id"),
						"budget", String.valueOf(row.getDouble("budget")));
				}
			}
		} else {
			jedis.hset("budget_" + row.getString("name").replaceAll(" ", "").toLowerCase() + "_camp_" + row.getLong("campaign_id"), "budget",
					String.valueOf(row.getDouble("budget")));
			jedis.hset("budget_" + row.getString("name").replaceAll(" ", "").toLowerCase() + "_camp_" + row.getLong("campaign_id"), "budget_left",
					String.valueOf(row.getDouble("budget")));
			jedis.hset("budget_" + row.getString("name").replaceAll(" ", "").toLowerCase() + "_camp_" + row.getLong("campaign_id"), "reference",
					"0");
		}
	}

}
