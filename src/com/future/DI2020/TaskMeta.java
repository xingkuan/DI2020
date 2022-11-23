package com.future.DI2020;

import java.math.BigDecimal;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.logging.log4j.LogManager;

import java.sql.*;
import java.sql.Date;
import java.text.SimpleDateFormat;

/*
 class Meta
 
 Meta, singleton class of replication metat data, stored in RDBMS
 kafka.docx
 su � postgres
*/

class TaskMeta {

	private String jobID;
	private int taskID;
	private int actID;

	private int currState;
	
	Timestamp tsThisRefesh;

	private Connection repConn;
	private Statement repStmt;
	private ResultSet repRSet;

	private String keyDataType;

	private int fldCnt;

	private Timestamp tsLastAudit;

	private int poolID;
	private long startMS;
	private long endMS;

	private String srcTblAb7;

	private String lName, jName;

	private Timestamp tsThisRef;
	private long seqThisRef;

	private static final Logger logger = LogManager.getLogger();

	private static final Matrix metrix = Matrix.getInstance();
	private DataPointMgr dataMgr = DataPointMgr.getInstance();

	// encapsulate the details into tskDetailJSON;
	private JSONObject xfmDetailJSON;
	//private JSONObject tskDetailJSON;
	private Map tskDetail;
	private JSONObject dccDetailJSON;

	private JSONObject srcInstr, tgtInstr, dccInstr;

	ArrayList<Integer> fldType = new ArrayList<Integer>();
	ArrayList<String> fldNames = new ArrayList<String>();
	
	int totalDelCnt, totalInsCnt, totalErrCnt, totalMsgCnt;
	
	private static TaskMeta instance = null; // use lazy instantiation ;

	public static TaskMeta getInstance() {
		if (Objects.isNull(instance)) {
			instance = new TaskMeta();
			instance.initMeta();
		}
		return instance;
	}

	private TaskMeta() {}
	private void initMeta() {
		Conf conf = Conf.getInstance();
		String uID = conf.getConf("repDBuser");
		String uPW = conf.getConf("repDBpasswd");
		String url = conf.getConf("repDBurl");
		String dvr = conf.getConf("repDBdriver");

		try {
			Class.forName(dvr);
		} catch (ClassNotFoundException e) {
			logger.error("DB Driver error has occured");
			logger.error(e);
		}

		try {
			repConn = DriverManager.getConnection(url, uID, uPW);
			repConn.setAutoCommit(false);
			repStmt = repConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
		} catch (SQLException e) {
			logger.error(e);
		}
	}

	//actId	11	registration
	//		12	initialization
	//		15:	incremental processing
	//		19:	audit
	public int setupTask(String jID, int tskID, int actId) {
		int rtc;
		
		jobID = jID;
		taskID = tskID;
		actID = actId;

		Calendar cal = Calendar.getInstance();
		startMS = cal.getTimeInMillis();

		
		xfmDetailJSON=null;
		//tskDetailJSON=null;
		tskDetail=null;
		dccDetailJSON=null;
		srcInstr=null;
		tgtInstr=null;
		dccInstr=null;

		//curr_state
		// 		-1	(DB value)task is active. setupTask() will set DB value to -1; endTask() to 2.  
		//		0	(DB value)need initialized  
		//		2	(DB value)task can be invoked.
		//currState= (int) tskDetailJSON.get("curr_state");
		currState= (int) tskDetail.get("CURR_ST");
		if (currState>10) {
			logger.warn("	This task is active.");
			return -1;
		}

		
		JSONArray jo;
		String sql = "select task_id as DITASKID, src_db_id as DISRCDB, "
				+ "substring(src_tbl, 1, position('.' in src_tbl) as DISRCSCH, "
				+ "substring(src_tbl, position('.' in src_tbl) as DISRCTBL, "
				+ "tgt_db_id as DITGTDB, "
				+ "substring(tgt_tbl, 1, position('.' in tgt_tbl) as DITGTSCH, "
				+ "substring(tgt_tbl, position('.' in tgt_tbl) as DITGTTBL, "
				+ "b.instruct as instruct"
				+ "from task a, DATA_POINT b " + " where a.src_db_id=b.db_id and task_id=" + taskID;
		jo = SQLtoJSONArray(sql);
		if(jo.isEmpty()) {
			logger.error("task does not exist.");
			return -1;
		}
		//tskDetailJSON = (JSONObject) jo.get(0);
		tskDetail =  (Map) jo.get(0);
		String instStr=(String) tskDetail.get("instruct");
		JSONObject jo1=stringToJSONObject(instStr);
		String cdcDB=(String) jo1.get("cdcDB");
		String cdcTblTmplt=(String) jo1.get("cdcTbl");
		String cdcTbl=(String) cdcTblTmplt.replaceAll("<SRCTBL>", (String) tskDetail.get("DISRCTBL"));
		
		tskDetail.remove("instruct");

		updateTaskState(-1);
		
		return 0;
	}

	
	public JSONArray getDCCsByPoolID(int poolID) {
	String sql = "select src_db_id, tgt_db_id, src_jurl_name from task where pool_id = " + poolID;

	JSONArray jRslt = SQLtoJSONArray(sql);
	return jRslt;
	}
	public JSONArray SQLtoJSONArray(String sql) {
		JSONArray rslt = null;
		JSONObject jsonObject = null;

		JSONObject jo = new JSONObject();
		Statement stmt = null;
		ResultSet rset = null;
		String column;
		Object value;
		try {
			rslt = new JSONArray();
			stmt = repConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
			rset = stmt.executeQuery(sql);

			ResultSetMetaData rsmd = rset.getMetaData();
			int columnCount = rsmd.getColumnCount();

			while (rset.next()){
				jsonObject = new JSONObject();
				for (int index = 1; index <= columnCount; index++) {
					column = rsmd.getColumnName(index);
					value = rset.getObject(column);
					if (value == null) {
						jsonObject.put(column, "");
					} else if (value instanceof Integer) {
						jsonObject.put(column, (Integer) value);
					} else if (value instanceof String) {
						jsonObject.put(column, (String) value);
					} else if (value instanceof Boolean) {
						jsonObject.put(column, (Boolean) value);
					} else if (value instanceof Date) {
						jsonObject.put(column, ((Date) value).getTime());
					} else if (value instanceof Long) {
						jsonObject.put(column, (Long) value);
					} else if (value instanceof Double) {
						jsonObject.put(column, (Double) value);
					} else if (value instanceof Float) {
						jsonObject.put(column, (Float) value);
					} else if (value instanceof BigDecimal) {
						jsonObject.put(column, (BigDecimal) value);
					} else if (value instanceof Timestamp) {
						jsonObject.put(column, (Timestamp) value);
					} else if (value instanceof Byte) {
						jsonObject.put(column, (Byte) value);
					} else if (value instanceof byte[]) {
						jsonObject.put(column, (byte[]) value);
					} else {
						throw new IllegalArgumentException("Unmappable object type: " + value.getClass());
					}	
				}
				rslt.add(jsonObject);
			}
			rset.close();
			stmt.close();
		} catch (SQLException e) {
			rslt=null;
			logger.error(e);
		} 
		return rslt;
	}

	public JSONObject getXfrmDetails() {
		return xfmDetailJSON;
	}
	
	public Map getTaskDetails() {
		//return tskDetailJSON;
		return tskDetail;
	}

	public void endTask() {
		//first save the meta data of this task
		Calendar cal = Calendar.getInstance();
		endMS = cal.getTimeInMillis();
		
		updateTaskState(2);
		
		//then clear up the states, so the next task will not be confused.
		taskID = -1;
		actID=-1;
		currState=-1;
		
		
		jobID=null;
		tsThisRefesh=null;

		try {
			repStmt.close();
			repRSet.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		keyDataType=null;

		fldCnt=0;

		tsLastAudit=null;

		poolID = -1;
		startMS=0l;
		endMS=0l;

		srcTblAb7=null;

		lName=null; jName=null;

		tsThisRef=null;
		seqThisRef=0;


		xfmDetailJSON=null;
		//tskDetailJSON=null;
		tskDetail=null;
		dccDetailJSON=null;
		
	
		fldType = null;
		fldNames = null;
		
		totalDelCnt=0; totalInsCnt=0; totalErrCnt=0; totalMsgCnt=0;
	}

	private void updateTaskState(int st) {
		String sql = "update task set curr_state = " + st 
				+ " where task_id = " + taskID;
		runUpdateSQL(sql);
	}
	public void saveInitStats() {
		//markEndTime();
		int duration = (int) (endMS - startMS) / 1000;
		logger.info("    " + " duration: " + duration + " sec");

		// report to InfluxDB:
		metrix.sendMX(
				"duration,jobId=" + jobID + ",taskID=" + taskID + "~" + srcTblAb7 + " value=" + duration + "\n");
		metrix.sendMX(
				"insCnt,jobId=" + jobID + ",taskID=" + taskID + "~" + srcTblAb7 + " value=" + totalInsCnt + "\n");
		metrix.sendMX(
				"delCnt,jobId=" + jobID + ",taskID=" + taskID + "~" + srcTblAb7 + " value=" + totalDelCnt + "\n");
		metrix.sendMX(
				"errCnt,jobId=" + jobID + ",taskID=" + taskID + "~" + srcTblAb7 + " value=" + totalErrCnt + "\n");
		metrix.sendMX(
				//"JurnalSeq,jobId=" + jobID + ",taskID=" + taskID + "~" + srcTblAb7 + " value=" + miscValues.get("thisJournalSeq") + "\n");
				"JurnalSeq,jobId=" + jobID + ",taskID=" + taskID + "~" + srcTblAb7 + " value=" + seqThisRef + "\n");

		// Save to MetaRep:
		//java.sql.Timestamp ts = new java.sql.Timestamp(System.currentTimeMillis());
		String sql = "update task set init_dt = now() "
				+ ", init_duration = " + duration 
				//+ ", curr_state = " + currState
				//+ " seq_last_seq = " + miscValues.get("thisJournalSeq")
				+ ", seq_last_ref = " + seqThisRef
				+ " where task_id = " + taskID;
		runUpdateSQL(sql);
	}
	private void saveSyncStats() {
		//markEndTime();
		int duration = (int) (endMS - startMS) / 1000;
		logger.info(jobID + " duration: " + duration + " sec");

		// report to InfluxDB:
		metrix.sendMX(
				"duration,jobId=" + jobID + ",taskID=" + taskID + "~" + srcTblAb7 + " value=" + duration + "\n");
		metrix.sendMX(
				"insCnt,jobId=" + jobID + ",taskID=" + taskID + "~" + srcTblAb7 + " value=" + totalInsCnt + "\n");
		metrix.sendMX(
				"delCnt,jobId=" + jobID + ",taskID=" + taskID + "~" + srcTblAb7 + " value=" + totalDelCnt + "\n");
		metrix.sendMX(
				"errCnt,jobId=" + jobID + ",taskID=" + taskID + "~" + srcTblAb7 + " value=" + totalErrCnt + "\n");
		metrix.sendMX(
				//"JurnalSeq,jobId=" + jobID + ",taskID=" + taskID + "~" + srcTblAb7 + " value=" + miscValues.get("thisJournalSeq") + "\n");
				"JurnalSeq,jobId=" + jobID + ",taskID=" + taskID + "~" + srcTblAb7 + " value=" + seqThisRef + "\n");

		// Save to MetaRep:
		//java.sql.Timestamp ts = new java.sql.Timestamp(System.currentTimeMillis());
		String sql = "update task set"
				+ " curr_sate = " + currState
				+ " ts_last_ref = now(),"
				//+ " seq_last_seq = " + miscValues.get("thisJournalSeq")
				+ " seq_last_ref = " + seqThisRef
				+ " where task_id = " + taskID;
		runUpdateSQL(sql);
	}

	private boolean runUpdateSQL(String sql) {
		boolean isok=true;
		// Save to MetaRep:
		//java.sql.Timestamp ts = new java.sql.Timestamp(System.currentTimeMillis());
		Statement stmt=null; 
		try {
			stmt = repConn.createStatement();
			int rslt = stmt.executeUpdate(sql);
			stmt.close();
			repConn.commit();
		} catch (SQLException e) {
			logger.error(e);
			isok=false;
		} 
		return isok;
	}

	private boolean runDBcmds(DataPoint db, JSONArray inst) {
		String sqlStr, type;
		JSONObject jo;
		int rslt;
		Iterator<JSONObject> it = inst.iterator();
		while (it.hasNext()) {
			jo = it.next();
			sqlStr= (String) jo.get("cmd");
			type= (String) jo.get("type");
			//System.out.println(sqlStr );

			sqlStr = parseStmt(sqlStr);  //replace place holders

System.out.println("cmd: " + sqlStr);
/*			rslt = db.runDBcmd(sqlStr, type);
			if(rslt < 0) {
				System.out.println("something is not right.");
				break;
			}
*/
		}

		return true;
	}

	public ArrayList<String> getFldNames() {
		return fldNames;
	}
	
	public void setRefreshTS(Timestamp thisRefreshHostTS) {
		tsThisRef = thisRefreshHostTS;
	}
	public void setRefreshSeqThis(long seq) {
		if (seq > 0) {  
			seqThisRef = seq;
		} else {   //should never happen. no?
			//seqThisRef = (long) miscValues.get("thisJournalSeq");
			seqThisRef = Long.valueOf( (dccDetailJSON.get("seq_last_ref").toString()));
			logger.info("... need to see why can't retrieve Journal Seq!!!");
		}
	}

	public String getJobID() {
		return jobID;
	}

	public Timestamp getLastAudit() {
		return tsLastAudit;
	}

	public long getDCCSeqLastRefresh() {
		try {
			return Long.valueOf(tskDetail.get("seq_last_ref").toString());
		}catch (NullPointerException e) {
			return -1;
		}
	}

	public int getPoolID() {
		return poolID;
	}

	public int getCurrState() {
		return currState;
	}

	public int getTableID() {
		return taskID;
	}

	public void close() {
		try {
			repRSet.close();
			repStmt.close();
			repConn.close();
		} catch (Exception e) {
			logger.warn("TODO: closed already. " + e);
		}

	}

/*	public String getLabel() {
		return jobID;
	}
*/
	public List<Integer> getTblsByPoolID(int poolID) {
		Statement lrepStmt = null;
		ResultSet lrRset;
		List<Integer> tList = new ArrayList<Integer>();
		String strSQL;

		if (poolID < 0)
			strSQL = "select task_id,curr_state from task order by 1";
		else
			strSQL = "select task_id,curr_state from task where pool_id = " + poolID + " order by 1";

		try {
			lrepStmt = repConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			lrRset = lrepStmt.executeQuery(strSQL);
			while (lrRset.next()) {
				// Retrieve by column name
				int id = lrRset.getInt(1);
				tList.add(id);
			}
		} catch (SQLException se) {
			logger.error("OJDBC driver error has occured" + se);
		} catch (Exception e) {
			// Handle errors for Class.forName
			logger.error(e);
		} finally {
			// make sure the resources are closed:
			try {
				if (lrepStmt != null)
					lrepStmt.close();
			} catch (SQLException se2) {
			}
		}

		return tList;
	}

	public List<Integer> getTableIDsAll() {
		return getTblsByPoolID(-1);
	}

// ... move to MetaData ?
	public void setThisRefreshHostTS() {
		tsThisRefesh = new Timestamp(System.currentTimeMillis());
	}

	public void setTotalMsgCnt(int v) {
		totalMsgCnt = v;
	}
	public void setTotalDelCnt(int v) {
		totalDelCnt = v;
	}

	public void setTotalInsCnt(int v) {
		totalInsCnt = v;
	}

	public void setTotalErrCnt(int v) {
		totalErrCnt = v;
	}

	public String getDCCPoolID() {
		// TODO Auto-generated method stub
		return null;
	}

	public String getAvroSchema(){
		String avroSchema;
		String avroFld, avroFlds="";
		
		String sql;
		
		//construct avroSchema from data_flds
		try {
			repStmt = repConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			//fist get the avro fild template
			sql = "select avro_fld_templ from db_engine where db_engine ='"+tskDetail.get("DISRDDBID")+"'";
			repRSet = repStmt.executeQuery(sql);
			repRSet.next();
			String sqlRslt = repRSet.getString(1);
			JSONObject avroTemplate = stringToJSONObject(sqlRslt);
			//now the the fields and copose the avro schema
			sql ="select data_flds from task where task_id = " + taskID;
			repRSet = repStmt.executeQuery(sql);
			repRSet.next();
			sqlRslt = repRSet.getString(1);

			JSONObject ja = stringToJSONObject(sqlRslt);
			JSONObject jo;
			for (int i = 0 ; i < ja.size(); i++) {
				   jo = (JSONObject) ja.get(i);
				   
				   avroFld = (String) avroTemplate.get(jo.get("type"));
				   avroFld.replaceFirst("<p>", (String) jo.get("precision"));
				   avroFld.replaceFirst("<s>", (String) jo.get("scale"));
				   avroFlds = avroFlds + avroFld + ",";
				   
		    }
		} catch (SQLException se) {
			logger.error("OJDBC driver error has occured" + se);
		} catch (Exception e) {
			// Handle errors for Class.forName
			logger.error(e);
		} 
		
		avroSchema = "{\"namespace\": \"com.future.DI2020.avro\", \n" 
			    + "\"type\": \"record\", \n" 
			    + "\"name\": \"" + tskDetail.get("DISRCTBL") + "\", \n" 
			    + "\"fields\": [ \n" 
			    + avroFlds + "] }";
		
		return avroSchema;
	}

	public String getDDLforEngine(String dbEngine){
		String crtTbl;
		String Fld, Flds="";
		
		String sql;
		
		//construct avroSchema from data_flds
		try {
			repStmt = repConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			//fist get the avro fild template
			sql = "select fld_templ from db_engine where db_engine ='"+dbEngine+"'";
			repRSet = repStmt.executeQuery(sql);
			repRSet.next();
			String sqlRslt = repRSet.getString(1);
			JSONObject avroTemplate = stringToJSONObject(sqlRslt);
			//now the the fields and copose the avro schema
			sql ="select data_flds from task where task_id = " + taskID;
			repRSet = repStmt.executeQuery(sql);
			repRSet.next();
			sqlRslt = repRSet.getString(1);

			JSONObject ja = stringToJSONObject(sqlRslt);
			JSONObject jo;
			for (int i = 0 ; i < ja.size(); i++) {
				   jo = (JSONObject) ja.get(i);
				   
				   Fld = (String) avroTemplate.get(jo.get("type"));
				   Fld.replaceFirst("<p>", (String) jo.get("precision"));
				   Fld.replaceFirst("<s>", (String) jo.get("scale"));
				   Flds = Flds + Fld + ",";
				   
		    }
		} catch (SQLException se) {
			logger.error("OJDBC driver error has occured" + se);
		} catch (Exception e) {
			// Handle errors for Class.forName
			logger.error(e);
		} 
		
		crtTbl = "create table " + tskDetail.get("DITGTTBLE") 
			    + "(" + Flds + ")";
		
		return crtTbl;
	}

	/**** task admin APIs ****/
	public void setupTask(String jobId, Map<String, String> vars) {
		int rtc;
		
		jobID = jobId;
		actID = 11;    //regist a new task
		taskID = -1;   //taskID is to be generated.
		
		//tskDetailJSON=new JSONObject(vars);
		tskDetail=vars;
		
		jobId = jobId + tskDetail.get("DISRCTBL");
	}

	public void disable() {
		DBMeta repoDB = DBMeta.getInstance();

		//DataPoint srcDB = dataMgr.getDB((String) (tskDetailJSON.get("DISRCDB")));
		//DataPoint tgtDB = dataMgr.getDB((String) (tskDetailJSON.get("DITGTDB")));;
		DataPoint srcDB = dataMgr.getDB((String) (tskDetail.get("DISRCDB")));
		DataPoint tgtDB = dataMgr.getDB((String) (tskDetail.get("DITGTDB")));;

		JSONObject jsonRslt;
		JSONObject jo;

		//src side
		jo = (JSONObject) repoDB.getDB("DISRCDB").get("isntruct");
		JSONArray stmts = (JSONArray) jo.get("disable");
		runDBcmds(srcDB, stmts);
		//tgt side
		jo = (JSONObject) repoDB.getDB("DITGTDB").get("instruct");
		stmts = (JSONArray) jo.get("disable");
		runDBcmds(tgtDB, stmts);
		
		String sql = "update task set .... ";
		runUpdateSQL(sql);
	}

	public void unregist() {
		DBMeta repoDB = DBMeta.getInstance();

		DataPoint srcDB = dataMgr.getDB((String) (tskDetail.get("DISRCDB")));
		DataPoint tgtDB = dataMgr.getDB((String) (tskDetail.get("DITGTDB")));;

		JSONObject jsonRslt;
		//src side
		JSONObject jo = (JSONObject) repoDB.getDB("DISRCDB").get("instruct");
		JSONArray stmts = (JSONArray) jo.get("unregist");
		runDBcmds(srcDB, stmts);
		//tgt side
		jo = (JSONObject) repoDB.getDB("DITGTDB").get("instr");
		stmts = (JSONArray) jo.get("unregist");
		runDBcmds(tgtDB, stmts);
		
		String sql = "update task set ...";
		runUpdateSQL(sql);
	}

	public int preRegist() {
		DBMeta repoDB = DBMeta.getInstance();

		String sql;
		JSONArray rslt;
		
		//if(!tskDetailJSON.get("DITASKID").equals("-1")) {
		if(!tskDetail.get("DITASKID").equals("-1")) {
			//taskID = Integer.parseInt((String) tskDetailJSON.get("DITASKID"));
			taskID = Integer.parseInt((String) tskDetail.get("DITASKID"));
		}else {
			taskID = getNextTaskID();
		}
		
		//verify taskID is not used
		sql = "select task_id from task where task_id = " + taskID ; 
		rslt = (JSONArray) SQLtoJSONArray(sql);
		if(rslt.size()>0) {
			//logger.error("   task ID is already used!");
			System.out.println("task ID is already used: " + taskID);
			return 01;
		}
		
		//String srcDBid = (String) (tskDetailJSON.get("DISRCDB"));
		//String tgtDBid = (String) (tskDetailJSON.get("DITGTDB"));
		String srcDBid = (String) (tskDetail.get("DISRCDB"));
		String tgtDBid = (String) (tskDetail.get("DITGTDB"));
		//pre-check DBs
		DataPoint srcDB = dataMgr.getDB(srcDBid);
		DataPoint tgtDB = dataMgr.getDB(tgtDBid);
		
		String sqlStr, type;
		JSONObject parm;
		
		//1. source side
		String instrStr;
		JSONObject instruJo; 
		instrStr = (String) repoDB.getDB(srcDBid).get("instruct");
		instruJo=stringToJSONObject(instrStr);
		JSONArray stmts = (JSONArray) instruJo.get("preRegist");
		if (stmts!=null)
			runDBcmds(srcDB, stmts);
		//2. tgt side
		instrStr = (String) repoDB.getDB(tgtDBid).get("instruct");
		instruJo=stringToJSONObject(instrStr);
		stmts = (JSONArray) instruJo.get("preRegist");
		if (stmts!=null) {
			boolean ok=runDBcmds(tgtDB, stmts);
			if(!ok)
				return -1;
		}
		//3. verify the objects is not registered 
		String mbr=(String) tskDetail.get("DITGTTBL");
		
		sql = "select task_id from task where SRC_DB_ID='" + tskDetail.get("DISRCDB") 
				+ "' and SRC_TBL='"	+ tskDetail.get("DISRCTBL") 
				+ "' and TGT_TBL like '%" + mbr + "%'";  //if mbr_lst is not null, it is something like DB2/as400 journal ...
		rslt = (JSONArray) SQLtoJSONArray(sql);
		if(rslt.size()>0) {
			logger.error("   the source is already registered!");
			return -1;
		}

		//done with srdData and tgtData
		//dataMgr.returnDB((String) (tskDetailJSON.get("DISRCDB")), srcDB);		
		//dataMgr.returnDB((String) (tskDetailJSON.get("DITGTDB")), tgtDB);		

		return taskID;
	}
	

	
	public boolean regist() {
		DBMeta repoDB = DBMeta.getInstance();

		boolean isOk;
		
		String srcDBid= (String) (tskDetail.get("DISRCDB"));
		String tgtDBid= (String) (tskDetail.get("DITGTDB"));
		DataPoint srcDB = dataMgr.getDB(srcDBid);
		DataPoint tgtDB = dataMgr.getDB(tgtDBid);

		//1. src side
		String instrStr = (String) repoDB.getDB(srcDBid).get("instruct");
		JSONObject instrJo = stringToJSONObject(instrStr);
		String bareSQL = (String) instrJo.get("bareSQL");
		bareSQL = parseStmt(bareSQL);
		Map taskStmts = srcDB.getRegSTMTs(bareSQL); //"srcQuery","tgtInsert","avro", "avro","fldCnt" need to be inserted into task
		
		JSONArray stmts = (JSONArray) instrJo.get("regist");
		if(stmts !=null) {
		isOk=runDBcmds(srcDB, stmts);
		if(!isOk) {
			System.out.println("comand failed on " + srcDBid);
			return false;
		}
		}
		//2. tgt side
		instrStr = (String) repoDB.getDB(tgtDBid).get("instruct");
		instrJo = stringToJSONObject(instrStr);
		stmts = (JSONArray) instrJo.get("regist");
		isOk =runDBcmds(tgtDB, stmts);
		if(!isOk) {
			System.out.println("comand failed on " + tgtDBid);
			return false;
		}

		//insert into task
		java.sql.Timestamp ts = new java.sql.Timestamp(System.currentTimeMillis());
		String insTask = "insert into task (task_id, pool_id, curr_state, "
				+ "src_db_id, src_tbl, src_stmt, fld_cnt, "
				+ "tgt_db_id, tgt_tbl, tgt_stmt, avro_schema, "
				+ "reg_dt) values (" 
				+ tskDetail.get("DITASKID") + ", 1," +tskDetail.get("DICURRST") + ", '"
				+ tskDetail.get("DISRCDB") + "', '" + tskDetail.get("DISRCTBL") + "',  '" 
//"srcQuery","tgtInsert","avro", "avro","fldCnt"
				+ taskStmts.get("srcQuery")  + "', " + taskStmts.get("fldCnt") + ", '" 
				+ tskDetail.get("DITGTDB") + "', '" + tskDetail.get("DITGTTBL") + "', '" 
				+ taskStmts.get("tgtInsert")  + "', '" + taskStmts.get("avro") + "', '" 
				+ ts +"')"; 
		return runUpdateSQL(insTask);	
	
	}

	private String parseStmt(String sqlStr) {
		String sqlStmt = sqlStr;
		String placehold, key, val;

		//String sqlStmt = "This is a TEST . another UPPER case";
		Pattern pattern = Pattern.compile("<[A-Z0-9]['A-Z0-9]+>");//, Pattern.CASE_INSENSITIVE);
		Matcher matcher = pattern.matcher(sqlStr);
		while ( matcher.find()) {
			//System.out.println("Found the text \"" + matcher.group()
			//+ "\" starting at " + matcher.start()
			//+ " index and ending at index " + matcher.end());
			placehold=matcher.group();
			key=placehold.replace("<", "");
			key=key.replace(">", "");
			val=(String) tskDetail.get(key);
			//System.out.println(placehold);
			//System.out.println(val);
			sqlStmt=sqlStmt.replace(placehold, val);
			//System.out.println(sqlStmt);
		}
		
		return sqlStmt;
	}

	private JSONObject stringToJSONObject(String str) {
		JSONObject rslt;
		JSONParser parser = new JSONParser();
		
		try {
			rslt = (JSONObject) parser.parse(str);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			rslt = null;
		}
		
		return rslt;
	}
	public int getNextTaskID() {
		Statement repStmt;
		ResultSet rRset;

		int taskID = 0;

		try {
			repStmt = repConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
			rRset = repStmt.executeQuery("select max(task_id) from task ");

			rRset.next();
			taskID = rRset.getInt(1);
			rRset.close();
			repStmt.close();
		} catch (SQLException e) {
			logger.error(e);
		}

		return taskID + 1;
	}


	/********** transformation metadata**************************/
	List<String> xFnList;
	public String getScripts() {
		String js="";
		xFnList=new ArrayList<String>();;
		
		String sql="select xform0 from xform_simple where x_id ="+actID;
		JSONObject jo=(JSONObject) SQLtoJSONArray(sql).get(0);
		String joStr = jo.get("xform0").toString();
		JSONParser parser = new JSONParser();
		
		JSONObject obj;
		try {
			JSONArray array = (JSONArray)parser.parse(joStr);
			for (int i = 0 ; i < array.size(); i++) {
			   obj = (JSONObject) array.get(i);
			   xFnList.add(obj.get("name").toString());
			   js = js +  obj.get("script")+"\n";
	    }
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return js;
	}
	public List<String> getxFnctList(){
		return xFnList;
	}

	public boolean isTaskReadyFor(int actId) {
		// TODO Auto-generated method stub
		return false;
	}

	public void setTaskState(int syncSt) {
		// TODO Auto-generated method stub
		currState=syncSt;
	}

}