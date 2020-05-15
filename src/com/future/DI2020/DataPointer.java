package com.future.DI2020;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
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
	public DataPointer(String dbID) throws SQLException {
	//protected DataPointer(String url, String cls, String user, String pwd) {
		JSONObject jo = metaData.readDBDetails(dbID);
		
		dbID=jo.get("db_id").toString();
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
						db = new DB2Data400(dbid);
						break;
					case "VERTICA":
						db = new VerticaData(dbid);
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
	//where clause compose of the parameter
	protected ResultSet getAuxResultSet(String qry) {
		return null;
	}
	//where clause is to be build from catalog
	protected ResultSet getAuxResultSet() {
		return null;
	}
	
	protected void initDataFrom(DataPointer dt) {
	}

	public boolean miscPrep() {
		totalErrCnt = 0; totalInsCnt = 0; totalDelCnt = 0; totalSynCnt=0;

		return true;
	}

	protected boolean ready() {
		// TODO Auto-generated method stub
		return false;
	}

	protected void close() {
		
	}

	protected int getRecordCount() {
		// TODO Auto-generated method stub
		return 0;
	}

	public void setupSinkData() {
		// TODO Auto-generated method stub
		
	}

	public ResultSet getSrcResultSet() {
		// TODO Auto-generated method stub
		return null;
	}
	public void syncDataFrom(DataPointer srcData) {
		// TODO Auto-generated method stub
		
	}
	public void syncDataVia(DataPointer srcData, DataPointer auxData) {
		// TODO Auto-generated method stub
		
	}
	
	public ResultSet getFieldMeta(String srcSch, String srcTbl, String journal){
		return null;
	}
	protected boolean regTblMisc(String srcSch, String srcTbl, String srcLog) {
		return false;
	}

	
}
