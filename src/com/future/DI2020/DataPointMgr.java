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
				String dbType=jo.get("db_type").toString();
				switch(dbType){
					case "DB2/AS400":
						db = new DB2Data400(jo);
						break;
					case "VERTICA":
						db = new VerticaData(jo);
						break;
					case "HIVE":
						db = new HiveData(jo);
						break;
					case "KAFKA":
						db = new KafkaData(jo);
						break;
					case "KAFKA_":
						db = new KafkaDCC(jo);
						break;
					case "ORACLE":
						db = new OracleData(jo);
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

	

	/********** Synch APIs****************************/
	protected int xformInto(DataPointMgr tgtData) {
		return 0;
	}
	/********** Synch APIs****************************/
	protected void crtSrcResultSet(List<String >keys) {
		logger.info("   Need implementation in child.");
	}
//	//where clause is to be build from catalog
//	protected int crtSrcResultSet(int actId, JSONArray jo) {
//		logger.info("   empty crtSrcAuxResultSet in DataPointer.");
//		return -1;
//	}
//	protected int crtSrcResultSet(int actId, JSONArray jo, DataPoint aux) {
//		logger.info("   empty crtSrcAuxResultSet in DataPointer.");
//		return -1;
//	}
	//where clause compose of the parameter
	protected ResultSet getSrcResultSet(String qry) {
		logger.info("   empty crtSrcAuxResultSet in DataPointer.");
		return null;
	}
//	protected ResultSet getSrcResultSet() {
//		logger.info("   empty crtSrcAuxResultSet in DataPointer.");
//		return null;
//	}

	protected int dropStaleRowsOfList(List<String> keys) {
		logger.info("   Need implementation in child.");
		return 0;
	}
	protected int getDccCnt() {
		logger.info("   Need implementation in child.");
		return 0;
	}

	protected void crtAuxSrcAsList() {
	}
	protected boolean crtSrcAuxResultSet() {
		logger.info("   empty crtSrcAuxResultSet in DataPointer.");
		return true;
	}
	protected List<String> getDCCKeyList(){
		logger.info("   empty getSrcResultList() in DataPointer.");
		return null;
	}
	protected void releaseRSandSTMT() {
		logger.info("   empty releaseRSandSTMT() in DataPointer.");
	}
	protected void commit() {
		logger.info("   should be implemented in child.");
	}
	protected void rollback() {
		logger.info("   should be implemented in child.");
	}
	/*********** Synch APIs ************/
	protected JSONObject  getSrcSqlStmts(String template) {
		return null;
	}
	public int crtSrcResultSet() {
		return -1;
	}
//	protected int initDataFrom(DataPoint dt) {
//		return 0;
//	}
	public void copyTo(DataPointMgr tgt) {
	}
	public void copyToVia(DataPointMgr tgt, DataPointMgr src) {
	}
	public void write(ResultSet rs) {  
	}
	public void write(GenericRecord rec) {  
	}
	public void write() {
	}
//	protected boolean ready() {
//		// TODO Auto-generated method stub
//		return false;
//	}
	protected boolean beginDCC(){
		logger.info("   should be implemented in child class.");
		return false;
	}
	//protected void afterSync(int actId, JSONArray aftSQLs){
	protected void afterSync(){
		logger.info("   should be implemented in child class.");
	}

	public int syncAvroDataFrom(DataPointMgr srcData) {
		// TODO Auto-generated method stub
		return 0;
	}
	public int syncDataFrom(DataPointMgr srcData) {
		// TODO Auto-generated method stub
		return 0;
	}
//	public int syncDataVia(DataPointer srcData, DataPointer auxData) {
//		// TODO Auto-generated method stub
//		return 0;
//	}
	public int syncDataViaV2(DataPointMgr srcData, DataPointMgr auxData) {
		// TODO Auto-generated method stub
		return 0;
	}
	
	public ResultSet getFieldMeta(String srcSch, String srcTbl, String journal){
		return null;
	}
	/******** register/unregister APIs ********/
	protected boolean regSrcCheck(int tblID, String PK, String srcSch, String srcTbl, String dccPgm, String jurl, String tgtSch, String tgtTbl, String dccDBid) {
		return false;
	}
	protected boolean regSrc(int tblID, String PK, String srcSch, String srcTbl, String dccPgm, String dccTbl, String tgtSch, String tgtTbl, String dccDBid){
		return false;
	}
	protected boolean regSrcDcc(int tblID, String PK, String srcSch, String srcTbl, String dccPgm, String dccTbl, String tgtSch, String tgtTbl, String dccDBid){
		return false;
	}
	protected boolean regDcc(int tblID, String PK, String srcSch, String srcTbl, String dccPgm, String dccTbl, String tgtSch, String tgtTbl, String dccDBid){
		return false;
	}
	protected boolean regTgt(int tblID, String PK, String srcSch, String srcTbl, String dccPgm, String dccTbl, String tgtSch, String tgtTbl, String dccDBid){
		return false;
	}
	protected boolean unregisterSrc(int tblID) {
		return false;
	}
	protected boolean unregisterTgt(int tblID) {
		return false;
	}
	protected boolean unregisterDcc(int tblID) {
		return false;
	}
	/***************************************/
	
	public long getThisRefreshSeq() {
		logger.info("   An empty method in DataPointer: getThisRefreshSeq()");
		// TODO Auto-generated method stub
		return 0;
	}
	
	protected void createKafkaConsumer() {
		// TODO Auto-generated method stub
		
	}
	public void write(JSONObject b) {
		// TODO Auto-generated method stub
		
	}


	
}
