package com.future.DI2020;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONObject;

public class DataPointer {
	protected String dbID;
	protected String URL;
	protected String driver;
	protected String userID;
	protected String passPWD;
	protected String dbType, dbCat;
	protected Connection dbConn;

	protected static final Logger ovLogger = LogManager.getLogger();
	protected static final MetaData metaData = MetaData.getInstance();
	protected static final Conf conf = Conf.getInstance();
	protected static final Metrix metrix = Metrix.getInstance();
	protected static final String logDir = conf.getConf("logDir");

	private static Map<String, DataPointer> dataPtrs=new HashMap<>();

	protected int totalErrCnt = 0, totalInsCnt = 0, totalDelCnt = 0, totalSynCnt=0;

	public DataPointer() {
        ovLogger.info("implicit DataPointer constructor.");
	}
	//public DataPointer(String dbid) throws SQLException {
	public DataPointer(JSONObject jo) throws SQLException {
		//JSONObject jo = metaData.readDBDetails(dbid);

		dbID=jo.get("db_id").toString();
		//this.dbID=dbid;
		URL=jo.get("db_conn").toString();
		driver=jo.get("db_driver").toString();
		userID=jo.get("db_usr").toString();
		passPWD=jo.get("db_pwd").toString();
		dbType=jo.get("db_cat").toString();
		dbType=jo.get("db_type").toString();
		dbCat=jo.get("db_cat").toString();
		connectDB();  
	}
	
	public static DataPointer dataPtrCreater(String dbid) {
		DataPointer db;
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
						db = new DB2Data400(jo);
						break;
					case "VERTICA":
						db = new VerticaData(jo);
						break;
					case "KAFKA":
						db = new KafkaData(jo);
						break;
					case "ORACLE":
						db = new OracleData(jo);
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
	private void connectDB() {
		if(dbCat.equals("RDBMS")){
		try {
            //Class.forName("oracle.jdbc.OracleDriver"); 
            Class.forName(driver); 
         } catch(ClassNotFoundException e){
            ovLogger.error("   Driver error has occured");
            ovLogger.error( e);
         }
      
      try {
         dbConn = DriverManager.getConnection(URL, userID, passPWD);
         dbConn.setAutoCommit(false);
      } catch(SQLException e) {
         ovLogger.error("   cannot connect to db");
         ovLogger.error(e);
      }
	}else {
		ovLogger.info("   not applicable for non-Relational.");
	}
	}
	protected void crtSrcResultSet(List<String >keys) {
	}
	protected void dropStaleRowsOfList(List<String> keys) {
	}

	public void crtAuxSrcAsList() {
	}
	protected boolean crtSrcAuxResultSet() {
		ovLogger.info("   empty crtSrcAuxResultSet in DataPointer.");
		return true;
	}
	//where clause is to be build from catalog
	protected int crtSrcResultSet(String str) {
		ovLogger.info("   empty crtSrcAuxResultSet in DataPointer.");
		return -1;
	}
	//where clause compose of the parameter
	protected ResultSet getSrcResultSet(String qry) {
		ovLogger.info("   empty crtSrcAuxResultSet in DataPointer.");
		return null;
	}
	protected ResultSet getSrcResultSet() {
		ovLogger.info("   empty crtSrcAuxResultSet in DataPointer.");
		return null;
	}
	protected List<String> getSrcResultList(){
		ovLogger.info("   empty getSrcResultList() in DataPointer.");
		return null;
	}
	protected void releaseRSandSTMT() {
		ovLogger.info("   empty releaseRSandSTMT() in DataPointer.");
	}
	
	protected int initDataFrom(DataPointer dt) {
		return 0;
	}
	protected boolean miscPrep(String jobTemplate) {
		totalErrCnt = 0; totalInsCnt = 0; totalDelCnt = 0; totalSynCnt=0;
		return true;
	}
	protected void setupSink() {
		ovLogger.info("   An empty setupSink() in DataPointer.");
	}
	protected boolean ready() {
		// TODO Auto-generated method stub
		return false;
	}
	protected boolean beginDCC(){
		ovLogger.info("   should be implemented in child class.");
		return false;
	}

	protected void close() {
		
	}

	protected int getRecordCount() {
		// TODO Auto-generated method stub
		return 0;
	}

	public int syncDataFrom(DataPointer srcData) {
		// TODO Auto-generated method stub
		return 0;
	}
	public int syncDataVia(DataPointer srcData, DataPointer auxData) {
		// TODO Auto-generated method stub
		return 0;
	}
	public int syncDataViaV2(DataPointer srcData, DataPointer auxData) {
		// TODO Auto-generated method stub
		return 0;
	}
	
	public ResultSet getFieldMeta(String srcSch, String srcTbl, String journal){
		return null;
	}
	protected boolean regTblMisc(String srcSch, String srcTbl, String srcLog) {
		return false;
	}
	public long getThisRefreshSeq() {
		ovLogger.info("   An empty method in DataPointer: getThisRefreshSeq()");
		// TODO Auto-generated method stub
		return 0;
	}

	
	// ......
	protected JSONObject genRegSQLs(int tblID, String PK, String srcSch, String srcTbl, String dccPgm, String dccTbl, String tgtSch, String tgtTbl, String dccDBid){
		return null;
	}
	
}
