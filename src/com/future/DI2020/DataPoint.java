package com.future.DI2020;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class DataPoint {
	protected String dbRole;
	
	protected String dbID;
	protected String urlString;
	protected String driver;
	protected String userID;
	protected String passPWD;
	protected String dbType, dbCat;

	public static final Logger logger = LogManager.getLogger();
	protected static final MetaData metaData = MetaData.getInstance();
	protected static final Conf conf = Conf.getInstance();
	protected static final Metrix metrix = Metrix.getInstance();
	protected static final String logDir = conf.getConf("logDir");

	private static Map<String, DataPoint> dataPtrs=new HashMap<>();

	protected int totalErrCnt = 0, totalInsCnt = 0, totalDelCnt = 0, totalSynCnt=0;

	public DataPoint() {
        logger.info("implicit DataPointer constructor.");
	}
	//public DataPointer(String dbid) throws SQLException {
	public DataPoint(JSONObject jo, String role) {
		//JSONObject jo = metaData.readDBDetails(dbid);
		dbRole = role;
		
		dbID=jo.get("db_id").toString();
		//this.dbID=dbid;
		urlString=jo.get("db_conn").toString();
		driver=jo.get("db_driver").toString();
		userID=jo.get("db_usr").toString();
		passPWD=jo.get("db_pwd").toString();
		dbType=jo.get("db_cat").toString();
		dbType=jo.get("db_type").toString();
		dbCat=jo.get("db_cat").toString();
	}
	
	public String getDBType() {
		return dbType;
	}
	
	//role should be SRC, TGT or DCC
	public static DataPoint dataPtrCreater(String dbid, String role) {
		DataPoint db;
		db = dataPtrs.get("dbid");
		
		if(db != null) {
			return db;
		}else {
			try {
				//db = new DataPointer(dbid);
				JSONObject jo = metaData.readDBDetails(dbid);
				String dbType=jo.get("db_type").toString();
				switch(dbType){
					case "DB2/AS400":
						db = new DB2Data400(jo, role);
						break;
					case "VERTICA":
						db = new VerticaData(jo, role);
						break;
					case "KAFKA":
						db = new KafkaData(jo, role);
						break;
					case "KAFKA_":
						db = new KafkaDCC(jo, role);
						break;
					case "ORACLE":
						db = new OracleData(jo, role);
						break;
					case "ES":
						db = new ESData(jo, role);
						break;
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			dataPtrs.put(dbid, db);
		}
		return db;
	}
	/********** Synch APIs****************************/
	protected int xformInto(DataPoint tgtData) {
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
	//protected boolean miscPrep(String jobTemplate) {
	protected boolean miscPrep() {
		totalErrCnt = 0; totalInsCnt = 0; totalDelCnt = 0; totalSynCnt=0;
		return true;
	}
	protected void setupSink() {
		logger.info("   An empty setupSink() in DataPointer.");
	}
	public void copyTo(DataPoint tgt) {
	}
	public void copyToVia(DataPoint tgt, DataPoint src) {
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
	protected void close() {
		
	}

	protected int getRecordCount() {
		// TODO Auto-generated method stub
		return 0;
	}
	public int syncAvroDataFrom(DataPoint srcData) {
		// TODO Auto-generated method stub
		return 0;
	}
	public int syncDataFrom(DataPoint srcData) {
		// TODO Auto-generated method stub
		return 0;
	}
//	public int syncDataVia(DataPointer srcData, DataPointer auxData) {
//		// TODO Auto-generated method stub
//		return 0;
//	}
	public int syncDataViaV2(DataPoint srcData, DataPoint auxData) {
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
