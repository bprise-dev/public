package com.bprise.orbit.migration.task;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import com.bprise.orbit.migration.handler.Handler;
import com.bprise.orbit.migration.util.Constants;
import com.bprise.orbit.migration.util.QueryStore;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import redis.clients.jedis.Jedis;

/**
 * The MigrationTask program implements Cassandra to Redis Migration Task
 * migrate only the updated data since from last schedule job run.
 * 
 * @author vishal.shelar
 *
 */
@Component
public class MigrationTask {

	private Handler adHandler;

	private Handler audienceHandler;

	private Handler campaignHandler;

	private Handler locationHandler;

	private Handler beaconHandler;

	private Handler vendorsHandler;

	private Handler adSpotHandler;

	private Handler vendorApplicationHandler;

	private Handler placeHandler;

	private Handler mediumHandler;

	private static final Logger LOGGER = LoggerFactory.getLogger(MigrationTask.class);

	@Autowired
	private Session session;

	@Autowired
	private Jedis jedis;

	public static enum ModuleType {
		ADS("ADS"), LOCATIONGROUP("LOCATIONGROUP"), CAMPAIGN("CAMPAIGN"), AUDIENCERULE("AUDIENCERULE"), BEACON_MAPPING(
				"BEACON_MAPPING"), VENDORS("VENDORS"), VENDOR_APPLICATION(
						"VENDOR_APPLICATION"), ADSPOTS("ADSPOTS"), PLACE_LOCATION("PLACE_LOCATION"), MEDIUM("MEDIUM");

		private final String moduleType;

		private ModuleType(String moduleType) {
			this.moduleType = moduleType;
		}

		public String getModuleType() {
			return moduleType;
		}
	}

	/**
	 * This api Responsible to Update the Redis data based on the updated data
	 * from cassandra.
	 * <p>
	 * It take care Module specific handler which in turn describe entire
	 * Migration details for that Module.
	 */
	public void mainTask(long nextUpdateTime) {
		PreparedStatement prepared = null;
		BoundStatement bound = null;
		try {
			// final long lastSyncTime = getLastSyncTime(this.session);
			final Map<String, List<Long>> moduleWiseMap = getChangeLogDetails(this.session);
			for (Map.Entry<String, List<Long>> moduleTypeAndIds : moduleWiseMap.entrySet()) {

				try {
					/*switch (ModuleType.valueOf(moduleTypeAndIds.getKey())) {*/
					switch (ModuleType.valueOf("LOCATIONGROUP")) {			
					case ADS:
						adHandler.update(moduleTypeAndIds.getValue(), this.session, jedis);
						insertModuleWiseUpdates(nextUpdateTime, moduleTypeAndIds.getKey());
						break;
					case LOCATIONGROUP:
						
					//	locationHandler.update(moduleTypeAndIds.getValue(), this.session, jedis);
						locationHandler.update(moduleWiseMap.get("LOCATIONGROUP"), this.session, jedis);
						insertModuleWiseUpdates(nextUpdateTime, moduleTypeAndIds.getKey());
						break;
					case CAMPAIGN:
						
						campaignHandler.update(moduleTypeAndIds.getValue(), this.session, jedis);						
						insertModuleWiseUpdates(nextUpdateTime, moduleTypeAndIds.getKey());
						break;
					case AUDIENCERULE:
						audienceHandler.update(moduleTypeAndIds.getValue(), this.session, jedis);
						insertModuleWiseUpdates(nextUpdateTime, moduleTypeAndIds.getKey());
						break;
					case BEACON_MAPPING:
						beaconHandler.update(moduleTypeAndIds.getValue(), this.session, jedis);
						insertModuleWiseUpdates(nextUpdateTime, moduleTypeAndIds.getKey());
						break;
					case VENDORS:
						vendorsHandler.update(moduleTypeAndIds.getValue(), this.session, jedis);
						insertModuleWiseUpdates(nextUpdateTime, moduleTypeAndIds.getKey());
						break;
					case ADSPOTS:
						adSpotHandler.update(moduleTypeAndIds.getValue(), this.session, jedis);
						insertModuleWiseUpdates(nextUpdateTime, moduleTypeAndIds.getKey());
						break;
					case VENDOR_APPLICATION:
						vendorApplicationHandler.update(moduleTypeAndIds.getValue(), this.session, jedis);						
						insertModuleWiseUpdates(nextUpdateTime, moduleTypeAndIds.getKey());
						break;
					case PLACE_LOCATION:
						placeHandler.update(moduleTypeAndIds.getValue(), this.session, jedis);
						insertModuleWiseUpdates(nextUpdateTime, moduleTypeAndIds.getKey());
						break;
					case MEDIUM:
						mediumHandler.update(moduleTypeAndIds.getValue(), this.session, jedis);
						insertModuleWiseUpdates(nextUpdateTime, moduleTypeAndIds.getKey());
						break;
					default:
						break;
					}
				} catch (Exception e) {
					prepared = this.session.prepare(QueryStore.SCHEDULE_STATUS_INSERT);
					bound = prepared.bind(new Date(nextUpdateTime), new Date(new Date().getTime()), Constants.FAILED,
							moduleTypeAndIds.getKey());
					this.session.execute(bound);
					LOGGER.error("ERROR: Migration api error for module --> " + moduleTypeAndIds.getKey(), e);
					e.printStackTrace();
				}
			}
		} catch (Exception e) {
			LOGGER.error("ERROR: Migration api error --> ", e);
			LOGGER.info("INFO: Migration job has failed");
		} finally {

			this.session.getCluster().close();
		}
	}

	private void insertModuleWiseUpdates(long nextUpdateTime, String moduleType) {
		PreparedStatement prepared = null;
		BoundStatement bound = null;
		prepared = this.session.prepare(QueryStore.SCHEDULE_STATUS_INSERT);
		bound = prepared.bind(new Date(nextUpdateTime), new Date(new Date().getTime()), Constants.COMPLETED,
				moduleType);
		this.session.execute(bound);
		LOGGER.info("INFO: Migration job has Completed successfully for module "+moduleType);
	}

	/*
	 * public long getLastSyncTime(final Session session) { ResultSet rs =
	 * this.session.execute(QueryStore.LAST_SYNC_TIME_QUERY); return
	 * rs.one().getTimestamp(0).getTime(); }
	 */

	public Map<String, List<Long>> getChangeLogDetails(final Session session) throws ParseException {

		Map<String, List<Long>> moduleWiseMap = new TreeMap<String, List<Long>>();
		for (ModuleType moduleType : ModuleType.values()) {
			PreparedStatement preparedCassandraSync = this.session.prepare(QueryStore.LAST_SYNC_CASSANDRA_QUERY);
			BoundStatement boundCassandraSync = preparedCassandraSync.bind(moduleType.getModuleType());
			ResultSet rsCassandraSync = this.session.execute(boundCassandraSync);
			Row row1  = rsCassandraSync.one();
			if (row1 != null) {
				/*SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
				
				Date date = formatter.parse("2018-03-26T13:16:18.131");
				final long nextUpdateTime = date.getTime();*/
				PreparedStatement lastSync = this.session.prepare(QueryStore.LAST_SYNC_TIME_QUERY);
				BoundStatement boundLastSync = lastSync.bind(moduleType.getModuleType(),row1.getTimestamp("updated_time"));
				ResultSet rsLastSync = this.session.execute(boundLastSync);
				Row row2  = rsLastSync.one();
				if ( null != row2) {
					PreparedStatement prepared = this.session.prepare(QueryStore.CHANGE_LOG_QUERY);
					BoundStatement bound = prepared.bind(moduleType.getModuleType(),
							row2.getTimestamp("updated_time"));
					ResultSet rs = this.session.execute(bound);
					List<Row> rows = rs.all();
					if (rows != null && !rows.isEmpty()) {
						Set<Long> moduleIds = new HashSet<Long>();
						for (Row row : rows) {
							moduleIds.add(row.getLong(0));
						}
						if (!moduleIds.isEmpty()) {
							moduleWiseMap.put(moduleType.getModuleType(), new ArrayList<>(moduleIds));
						}
					}
				}
			}
		}
		return moduleWiseMap;
	}

	@Autowired
	@Qualifier("adHandler")
	public void setAdHandler(Handler adHandler) {
		this.adHandler = adHandler;
	}

	@Autowired
	@Qualifier("audienceHandler")
	public void setAudienceHandler(Handler audienceHandler) {
		this.audienceHandler = audienceHandler;
	}

	@Autowired
	@Qualifier("campaignHandler")
	public void setCampaignHandler(Handler campaignHandler) {
		this.campaignHandler = campaignHandler;
	}

	@Autowired
	@Qualifier("locationHandler")
	public void setLocationHandler(Handler locationHandler) {
		this.locationHandler = locationHandler;
	}

	@Autowired
	@Qualifier("beaconHandler")
	public void setBeaconHandler(Handler beaconHandler) {
		this.beaconHandler = beaconHandler;
	}

	@Autowired
	@Qualifier("vendorsHandler")
	public void setVendorsHandler(Handler vendorsHandler) {
		this.vendorsHandler = vendorsHandler;
	}

	@Autowired
	@Qualifier("adSpotHandler")
	public void setAdSpotHandler(Handler adSpotHandler) {
		this.adSpotHandler = adSpotHandler;
	}

	@Autowired
	@Qualifier("vendorApplicationHandler")
	public void setVendorApplicationHandler(Handler vendorApplicationHandler) {
		this.vendorApplicationHandler = vendorApplicationHandler;
	}

	@Autowired
	@Qualifier("placeHandler")
	public void setPlaceHandler(Handler placeHandler) {
		this.placeHandler = placeHandler;
	}

	@Autowired
	@Qualifier("mediumHandler")
	public void setMediumHandler(Handler mediumHandler) {
		this.mediumHandler = mediumHandler;
	}

}