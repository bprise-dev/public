package com.bprise.orbit.migration.util;

public class Constants {
	public static final String FAILED = "Failed";
	public static final String COMPLETED = "Completed";
	public static final String AD_CASSANDRA_ID = "ad_id";
	public static final String AD_REDIS_KEY = "ad_";
	public static final String AUDIENCE_RULE_CASSANDRA_ID = "audience_rule_id";
	public static final String AUD_REDIS_KEY = "aud_";
	public static final String CAMPAIGN_CASSANDRA_ID = "campaign_id";
	public static final String CAMPAIGN_REDIS_KEY = "camp_";
	public static final String LOCATION_GROUP_CASSANDRA_ID = "location_group_id";
	public static final String LOCATION_REDIS_KEYS = "loc_";
	public static final String TEMP_LOCATION_REDIS_KEYS = "temploc_";
	public static final String NO_AUDIENCE_CAMPAIGN_IDS_KEY="no_audience_campaigns";
	
	
	
	/********************************** Mqtt Constants*********************************************/
	public static final String MQTT_REDIS_KEY_AUTH_PREFIX="mqtt_user:";	
	public static final String MQTT_REDIS_KEY_ACL_PREFIX="mqtt_acl:";
	public static final String MQTT_TOKEN_STRING="password";
	public static final String MQTT_VENDER_KEYWORD="vendor/";
	public static final String MQTT_APP_KEYWORD="/app/";
	public static final String MQTT_DEVICE_KEYWORD="/device/+";
	public static final String MQTT_APP_TYPE="Mobile App";
	
	public static final int MQTT_BATCH_INSERTION=5;
	
/******************* AUDIENCE KEY CREATION CONSTANTS********************************************/
	
	/********************  BATCH SIZE ****************************/
	public static final int AUDIENCE_KEY_CRATION_BATCH_SIZE=10;
	
	/*********************** DB COLUMNS IN CASSANDRA **************/
	public static final String DB_COLUMN_NAME_AUD_RULE_ID="audience_rule_id";
	public static final String DB_COLUMN_NAME_AUD_RULE_DETAILS="aud_rules";
	public static final String DB_COLUMN_NAME_AUDIENCE_ID="audience_id";
	public static final String DB_COLUMN_NAME_CAMPAIGN_ID="campaign_id";
	public static final String DB_COLUMN_NAME_IS_DELETED="is_deleted";
	
	/********************** UDT OBJECT ATTRIBUTES *********************************/
	public static final String UDT_AUD_RULE_SET_ATT_ID_ATTRIBUTE="audience_rule_set_attribute_id";
	public static final String UDT_AUD_ATT_VALUE_ATTRIBUTE="attribute_value";
	public static final String UDT_AUD_STAUS_ATTRIBUTE="status";
	public static final String UDT_ATT_NAME_ATTRIBUTE="attribute_name";
	
	
	/******************** UDT OBJECT ATTRIBUTES PREDEFINED VALUES **************/
	public static final String INCLUDE_VALUE="Include";
	public static final String MIN_AGE_VALUE="Min_Age";
	public static final String MAX_AGE_VALUE="Max_Age";
	public static final String GENDER_VALUE="Gender";
	public static final String SIXTY_FIVE_PLUS="65+";
	
	
	/*********************** AUDIENCE KEY CREATION PREFIX FOR REDIS *********************/
	public static final String GENDER_KEY_PREFIX="G";
	public static final String AGE_KEY_PREFIX="A_";
	public static final String AUDIENCE_KEY_PREFIX="aud_";
	
	/*************************** REGEX PATTERN FOR AUDIENCE KEY CREATION ******************//*
	public static final String REGEX_FOR_GETTING_GENDER_PREFIX = "^[A-Za-z]";
	public static final String REGEX_TO_GET_MIN_AGE = "(\\d+)(?=_)";
	public static final String REGEX_TO_GET_MAX_AGE= "(\\d+)$";*/
	
	
	/*********** REGEX PATTERN FOR AUDIENCE KEY CREATION ***********/
	public static final String REGEX_FOR_GETTING_GENDER_PREFIX = "^[A-Za-z]";
	//public static final String REGEX_TO_GET_MIN_AGE = "(\\d+)(?=_)";
	public static final String REGEX_TO_GET_MIN_AGE = "(?:A_)(\\d+)(?=_)";
	//public static final String REGEX_TO_GET_MAX_AGE= "(\\d+)$";
	public static final String REGEX_TO_GET_MAX_AGE= "(\\d+)(?!.*\\d)";
	public static final String REGEX_TO_GET_GENDER_FROM_GROUP="(?:G)(\\D+)$";
	/************************ OTHER CONSTANTS*****************************/
	public static final String AGE_GENDER_SEPARATOR= "_";
}
