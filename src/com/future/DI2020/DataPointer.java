package com.future.DI2020;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
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
	protected String dbType;
	protected Connection dbConn;

	protected static final Logger ovLogger = LogManager.getLogger();
	protected static final MetaData metaData = MetaData.getInstance();
	protected static final Conf conf = Conf.getInstance();
	protected static final Metrix metrix = Metrix.getInstance();
	protected static final String logDir = conf.getConf("logDir");

	private static Map<String, DataPointer> dataPtrs;

	protected int totalErrCnt = 0, totalInsCnt = 0, totalDelCnt = 0, totalSynCnt=0;

	public DataPointer() {
        ovLogger.info("implicit DataPointer constructor.");
	}
	public DataPointer(String dbID) {
	//protected DataPointer(String url, String cls, String user, String pwd) {
		JSONObject jo = metaData.readDBDetails(dbID);
		
		dbID=jo.get("db_id").toString();
		URL=jo.get("db_url").toString();
		driver=jo.get("db_driver").toString();
		userID=jo.get("db_user").toString();
		passPWD=jo.get("passwd").toString();
		dbType=jo.get("db_type").toString();
		connectDB();  
	}
	
	public static DataPointer dataPtrCreater(String dbid) {
		DataPointer db;
		db = dataPtrs.get("dbid");
		
		if(db != null) {
			return db;
		}else {
			db = new DataPointer(dbid);

			dataPtrs.put(dbid, db);
		}
		return db;
	}
	private void connectDB() {
		if(dbType.equals("Relational")){
		try {
            //Class.forName("oracle.jdbc.OracleDriver"); 
            Class.forName(driver); 
         } catch(ClassNotFoundException e){
            ovLogger.error("   Driver error has occured");
            ovLogger.error( e);
         }
      
      try {
         close();
         //establish DB connection
         dbConn = DriverManager.getConnection(URL, userID, passPWD);
         dbConn.setAutoCommit(false);
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
	

	
}
