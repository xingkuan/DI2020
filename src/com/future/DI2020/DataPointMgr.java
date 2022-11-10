package com.future.DI2020;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class DataPointMgr implements AutoCloseable {
	//private static Map<String, DataPoint> dataPtrs=new HashMap<>();
	//TODO 20220926: same DataPoint can be requested multiple times. Need to 
	//		keep track of active and inactive ...
	//DBID_0: inactive; DBID_1: active
	//private static Map<String, ArrayList<DataPoint>> dataPtrs=new HashMap<>();
	private static Map<String, DataPoint> dataPtrs=new HashMap<>();

	private static final Logger logger = LogManager.getLogger();
//	private static final TaskMeta metaData = TaskMeta.getInstance();


	public DataPointMgr() {
	}
	
	private static DataPointMgr instance = null; // use lazy instantiation ;

	public static DataPointMgr getInstance() {
		if (instance == null) {
			instance = new DataPointMgr();
		}
		return instance;
	}

	
	//public DataPointer(String dbid) throws SQLException {
	
	//role should be SRC, TGT or DCC
	public DataPoint getDB(String dbid) {
		DataPoint db;

		db = dataPtrs.get(dbid);	
		
		if(db == null) {
			try {
			TaskMeta metaData = TaskMeta.getInstance();
			//db = new DataPointer(dbid);
			DBMeta repoDB = DBMeta.getInstance();
			JSONObject jo = (JSONObject) repoDB.getDB(dbid);
			String dbCat=jo.get("db_cat").toString();
			switch(dbCat){
				case "JDBC":
					db = new JDBCData(jo);
					break;
				case "MQK":
					db = new KafkaKey(jo);
					break;
				case "MQD":
					db = new KafkaData(jo);
					break;
				case "ES":
					db = new ESData(jo);
					break;
			}
			dataPtrs.put(dbid, db);		//add to the active list
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return db;
	}
	
	public static void returnDB(String dbid, DataPoint db) {
//		dataPtrs.get(dbid+"_1").remove(db);  //remove from active
//		dataPtrs.get(dbid+"_0").add(db);  //add to inactive
	}
	
	@Override
	public void close() throws Exception {
		for (DataPoint dp : dataPtrs.values()) 
			dp.closeDB();
	}
}