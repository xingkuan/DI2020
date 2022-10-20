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
	private static Map<String, ArrayList<DataPoint>> dataPtrs=new HashMap<>();

	private static final Logger logger = LogManager.getLogger();
	private static final TaskMeta metaData = TaskMeta.getInstance();


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
	public static DataPoint getDB(String dbid) {
		DataPoint db;

		db = dataPtrs.get(dbid+"_0").get(dataPtrs.get(dbid+"_0").size() - 1);	//get the last one
		
		if(db != null) {
			dataPtrs.get(dbid+"_0").remove(dataPtrs.get(dbid+"_0").size() - 1);  //remove from inactive
		}else {
			try {
				//db = new DataPointer(dbid);
				JSONObject jo = metaData.getDBDetails(dbid);
				String dbType=jo.get("db_cat").toString();
				switch(dbType){
					case "JDBC":
						db = new JDBCData(jo);
						break;
					case "MQ":
						db = new KafkaData(jo);
						break;
					case "ES":
						db = new ESData(jo);
						break;
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		if(null == dataPtrs.get(dbid+"_0")) {
			dataPtrs.put(dbid+"_0", new ArrayList<DataPoint>());
			dataPtrs.put(dbid+"_1", new ArrayList<DataPoint>());
		}
			
		dataPtrs.get(dbid+"_1").add(db);		//add to the active list
		return db;
	}
	
	public static void returnDB(String dbid, DataPoint db) {
		dataPtrs.get(dbid+"_1").remove(db);  //remove from active
		dataPtrs.get(dbid+"_0").add(db);  //add to inactive
	}
	
	@Override
	public void close() throws Exception {
		for (ArrayList<DataPoint> lst: dataPtrs.values()) {
			for(DataPoint d: lst) {
				d.closeDB();
			}
		}
	}

}
