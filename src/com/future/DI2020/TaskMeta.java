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
 su – postgres
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

	private static Map<String, JSONObject> instructions=new HashMap<>();

	// encapsulate the details into tskDetailJSON;
	private JSONObject xfmDetailJSON;
	private JSONObject tskDetailJSON;
	private JSONObject tmpDetailJSON;
	private JSONObject dccDetailJSON;

	private JSONObject srcInstr, tgtInstr, dccInstr;

	private JSONObject miscValues=new JSONObject();
	
	private String avroSchema;
	
	ArrayList<Integer> fldType = new ArrayList<Integer>();
	ArrayList<String> fldNames = new ArrayList<String>();
	
	int totalDelCnt, totalInsCnt, totalErrCnt, totalMsgCnt;
	
	private static TaskMeta instance = null; // use lazy instantiation ;

	public static TaskMeta getInstance() {
		if (instance == null) {
			instance = new TaskMeta();
		}
		return instance;
	}

	public TaskMeta() {
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
		tskDetailJSON=null;
		dccDetailJSON=null;
		srcInstr=null;
		tgtInstr=null;
		dccInstr=null;
		miscValues=null;

		//curr_state
		// 		-1	(DB value)task is active. setupTask() will set DB value to -1; endTask() to 2.  
		//		0	(DB value)need initialized  
		//		2	(DB value)task can be invoked.
		currState= (int) tskDetailJSON.get("curr_state");
		if (currState>10) {
			logger.warn("	This task is active.");
			return -1;
		}

		if(initTaskDetails() == -1 ) {  // tmpDetailJSON is included in initTableDetails();
			rtc = -1;
			return rtc;
		}
		
		initFieldMetaData();
		updateTaskState(-1);
		
		return 0;
	}

	public JSONObject getInstrs(String dbid) {
		return instructions.get(dbid);
	}
	
	private int initTaskDetails() {
		
		switch(actID) {
			case 12:	//remove
/*
oracle:
		String sql = "truncate table " + metaData.getTaskDetails().get("tgt_schema") + "."+ metaData.getTaskDetails().get("tgt_table");
		sql = "alter trigger " + dccPgm + " disable";
*/
				break;
			case 13:	//disable
/*
oracle:
  		String sql =  "drop TRIGGER " + metaData.getTaskDetails().get("src_dcc_pgm");
		executeSQL(sql);		
		sql="drop TABLE " + metaData.getTaskDetails().get("src_dcc_tbl");
				
 */
				break;
			case 14:	//initialization
/*
oracle:
				String sql="alter trigger "  + metaData.getTaskDetails().get("src_dcc_pgm").toString() + " disable";
		String sql = "truncate table " + metaData.getTaskDetails().get("tgt_schema") + "."+ metaData.getTaskDetails().get("tgt_table");
*/
				break;
			case 15:	//incremental processing
				break;
			case 19:	//audit
				break;
			default: 
				break;
		}
		
		JSONArray jo;
		String sql = "select task_id, template_id, data_pk, src_db_id, src_schema, src_table, tgt_db_id, tgt_schema, tgt_table, \n" + 
					"pool_id, init_dt, init_duration, curr_state, src_dcc_pgm, src_dcc_tbl, dcc_db_id, \n" + 
					"dcc_store, ts_regist, ts_last_ref, seq_last_ref, db_type,src_stmt0, tgt_stmt0 "
					+ " from task a, DATA_POINT b " + " where a.src_db_id=b.db_id and task_id=" + taskID;
		jo = SQLtoJSONArray(sql);
		if(jo.isEmpty()) {
			logger.error("task does not exist.");
			return -1;
		}
		tskDetailJSON = (JSONObject) jo.get(0);

		String templateId=tskDetailJSON.get("template_id").toString();
		
		switch(templateId) {
		case "XFRM":
			sql="select src_avro, tgt_avro "
					+ " from xform_simple " 
					+ " where x_id="+taskID;
			jo = SQLtoJSONArray(sql);
			if(jo.isEmpty()) {
				logger.error("error in DCC, e. g. DB2/AS400 journal");
				return -1;
			}
			xfmDetailJSON = (JSONObject) jo.get(0);
			break;
		default:
			if((actID==-1)||(actID==21)) {  //no further setup if it is unregistering or testing.
				return 0;
			}else {
				sql= "select template_id, act_id, info, stmts from TASK_TEMPLATE where template_id='" 
							+ tskDetailJSON.get("template_id") + "' and act_id=" + actID;
				jo = SQLtoJSONArray(sql);
				if(jo.isEmpty()) {
					logger.error("action not applicable.");
					return -1;
				}
				tmpDetailJSON = (JSONObject) jo.get(0);
			
				//TODO: not pretty here!
				if(templateId.equals("DATA_")) {  
					String journalName=tskDetailJSON.get("src_dcc_tbl").toString();
					String[] temp = journalName.split("\\.");
					lName=temp[0]; jName=temp[1];
					
					sql="select task_id, src_db_id, tgt_db_id, src_schema, src_table, seq_last_ref, ts_last_ref, curr_state "
							+ " from task " 
							+ " where src_db_id='" + tskDetailJSON.get("src_db_id") + "' and src_schema='"
							+ lName + "' and src_table='" + jName + "' and tgt_schema='*'";
					jo = SQLtoJSONArray(sql);
					if(jo.isEmpty()) {
						logger.error("error in DCC, e. g. DB2/AS400 journal");
						return -1;
					}
					dccDetailJSON = (JSONObject) jo.get(0);
				}
			}
			break;
		}
/*TODO 20220929		 move a lot of code into meta
oracle examples:
		JSONObject jo = new JSONObject();
		JSONArray pre = new JSONArray();
		switch(template) {
			case "1DATA":    //case: read the whole table
				pre.add(metaData.getBareSrcSQL() );
				jo.put("PRE", pre);
				break;
			case "2DATA":   //read the changed rows. Original O2V, O2K
			// Not needed as it is done in getDccCnt()	
			//	pre.add("update " + metaData.getTaskDetails().get("src_dcc_tbl") 
			//			+ " set dcc_ts = TO_TIMESTAMP('2000-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')" );
				pre.add(metaData.getBareSrcSQL() + ", " + metaData.getTaskDetails().get("src_dcc_tbl") 
						+ " b where b.dcc_ts = TO_TIMESTAMP('2000-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS') "
						+ " and a.rowid=b."+metaData.getTaskDetails().get("data_pk"));
				jo.put("PRE", pre);
				JSONArray aft = new JSONArray();
				aft.add("delete from " + metaData.getTaskDetails().get("src_dcc_tbl") 
						+ " where dcc_ts = TO_TIMESTAMP('2000-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')" );
				jo.put("AFT", aft);
				break;

DB2:
	protected JSONObject getSrcSqlStmts(String template) {
	//from metaData private JSONObject getO2Vact2SQLs() {
		JSONObject jo = new JSONObject();
		JSONArray pre = new JSONArray();
		switch(template) {
		case "1DATA":    //no case yet. Having it here as a remind.
		case "1DATA_":    //case: read the whole table
			pre.add( metaData.getBareSrcSQL() );
			jo.put("PRE", pre);
			break;
		case "2DATA":    //no case yet. Having it here as a remind.
			break;
		case "2DATA_":
			String sql ="DECLARE GLOBAL TEMPORARY TABLE qtemp.DCC"+ metaData.getTaskDetails().get("task_id") 
			+ "(" + metaData.getTaskDetails().get("data_pk") + " " + metaData.getKeyDataType() + ") " 
					+" NOT LOGGED"; 
			pre.add(sql);
			pre.add("INSERT INTO qtemp.DCC" + metaData.getTaskDetails().get("task_id") + " VALUES (?)" );
			sql = metaData.getBareSrcSQL() + ", qtemp.DCC"+metaData.getTaskDetails().get("task_id") + " b "
					+ " where a..rrn(a)=b." +metaData.getTaskDetails().get("data_pk");  //TOTO: may have problem!
			pre.add(sql);
			jo.put("PRE", pre);
			break;
		case "2DCC":
			sql=DB2DCCsql(true);
			pre.add(sql );
			jo.put("PRE", pre);
		}
		return jo;
	}

Vertica:
		String delSQL = "delete from " + metaData.getTaskDetails().get("tgt_schema") + "." + metaData.getTaskDetails().get("tgt_table") 
		+ " where " + metaData.getTaskDetails().get("data_pk") + "=?";

		String sql = "truncate table " + metaData.getTaskDetails().get("tgt_schema") + "."+ metaData.getTaskDetails().get("tgt_table");

		metaData.getSQLInsTgt()
		
Kafka:
		topic=metaData.getTaskDetails().get("tgt_schema")+"."+metaData.getTaskDetails().get("tgt_table");
		
		String jsonSch = metaData.getAvroSchema();
		schema = new Schema.Parser().parse(jsonSch); //TODO: ??  com.fasterxml.jackson.core.JsonParseException


	public void write(ResultSet rs) {
 	   record = new GenericData.Record(schema);	     //each record also has the schema ifno, which is a waste!   
 	   try {
		for (int i = 0; i < fldType.size(); i++) {  //The last column is the internal key.
//			* Can't use getObject() for simplicity. :(
//			 *   1. Oracle ROWID, is a special type, not String as expected
//			 *   2. For NUMBER, it returns as BigDecimal, which Java has no proper way for handling and 
//			 *      AVRO has problem with it as well.
//			 *
			//record.put(i, rs.getObject(i+1));
			switch(fldType.get(i)) {
			case 1:
				record.put(i, rs.getString(i+1));
				break;
			case 4:
				record.put(i, rs.getLong(i+1));
				break;
			case 7:
				tempO=rs.getDate(i+1);
				if(tempO==null)
					record.put(i, null);
				else {
					//tempNum = rs.getDate(i+1).getTime();
					record.put(i, tempO.toString());
				}
				break;
			case 6:
				tempO=rs.getTimestamp(i+1);
				if(tempO==null)
					record.put(i, null);
				else {
					//record.put(i, tempO); // class java.sql.Date cannot be cast to class java.lang.Long
					//tempNum = rs.getDate(i+1).getTime();
					//record.put(i, new java.util.Date(tempNum));  //class java.util.Date cannot be cast to class java.lang.Number 
					//record.put(i, tempNum);  //but that will show as long on receiving!
					record.put(i, tempO.toString());  
				}
		//		break;
		//	case 6:
		//		record.put(i, new java.util.Timestamp(rs.getTimestamp(i+1).getTime()));
				break;
			default:
				logger.warn("unknow data type!");
				record.put(i, rs.getString(i+1));
				break;
			}
			
		}
   		byte[] myvar = avroToBytes(record, schema);
   		//producer.send(new ProducerRecord<Long, byte[]>("VERTSNAP.TESTOK", (long) 1, myvar),new Callback() {
   		//producer.send(new ProducerRecord<Long, byte[]>(topic, (long) 1, myvar),new Callback() {  //TODO: what key to send?
   		producer.send(new ProducerRecord<Long, byte[]>(topic,  myvar),
   			new Callback() {             //      For now, no key
   				public void onCompletion(RecordMetadata recordMetadata, Exception e) {   //execute everytime a record is successfully sent or exception is thrown
   					if(e == null){
   						}else{
   							logger.error(e);
   						}
   					}
   			});
   			msgCnt++;
 	   	}catch (SQLException e) {
		  logger.error(e);
 	   	}
	}

*/		
		return 0;
	}
	
	/*2022.10.01
	 *same database can have multiple entries in DATA_POINT. source and target 
	 * will each has its own entry;
	 * also, sync as table and sync as SQL will have seperate entry.
	 */
	public JSONObject getDBlvlInstr(String dbid) {
		String sql= "select instruction "
					+ " from DATA_POINT " + " where db_id='" + dbid + "'";
		JSONObject jo = (JSONObject) SQLtoJSONArray(sql).get(0);
		return jo;
	}

	public JSONObject getItemlvlInstr(int dbid) {
		String sql= "select instruction "
					+ " from DATA_POINT " + " where db_id='" + dbid + "'";
		JSONObject jo = (JSONObject) SQLtoJSONArray(sql).get(0);
		return jo;
	}

	
	// return db details as a simpleJSON object, (instead of a cumbersome POJO).
	// used only by DataPointMgr
	public JSONObject getDBDetails(String dbid) {
		String sql= "select db_id, db_cat, db_type, db_conn, db_driver, "
				+ "db_usr, db_pwd "
					+ " from DATA_POINT " + " where db_id='" + dbid + "'";
		JSONObject jo = (JSONObject) SQLtoJSONArray(sql).get(0);
		return jo;
	}
	
	public JSONArray getDCCsByPoolID(int poolID) {
	String sql = "select src_db_id, tgt_db_id, src_jurl_name from task where pool_id = " + poolID;

	JSONArray jRslt = SQLtoJSONArray(sql);
	return jRslt;
	}
	public JSONArray SQLtoJSONArray(String sql) {
		JSONArray jArray = new JSONArray();
		JSONObject jsonObject = null;

		JSONObject jo = new JSONObject();
		Statement stmt = null;
		ResultSet rset = null;
		String column;
		Object value;
		try {
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
				jArray.add(jsonObject);
			}
			rset.close();
			stmt.close();
		} catch (SQLException e) {
			logger.error(e);
		} 
		return jArray;
	}

	public JSONObject getXfrmDetails() {
		return xfmDetailJSON;
	}
	public JSONObject getTaskDetails() {
		return tskDetailJSON;
	}
	/*
	public JSONObject getActDetails() {
		return tmpDetailJSON;
	}
	public JSONObject getMiscValues() {
		return miscValues;
	}
	public String getKeyDataType() {
		return keyDataType;
	}
	
	public boolean taskHasDependency(String dbID, String srcSch, String srcTbl) {
		boolean rtc=true;
		String sql="select 1 from task "
				+ "where src_db_id='"+dbID+"' "
				+ " and src_dcc_tbl='"+srcSch+"."+srcTbl + "'";
		JSONArray jo = SQLtoJSONArray(sql);
		if((jo==null)||jo.isEmpty()) {
			rtc=false;
		}
		return rtc;
	}

*/
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
		tskDetailJSON=null;
		tmpDetailJSON=null;
		dccDetailJSON=null;
		miscValues=null;
		
		avroSchema=null;
		
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
			} 
		return true;
	}

	private boolean runDBcmds(DataPoint db, JSONArray inst) {
		String sqlStr, type;
		JSONObject rslt;
		Iterator<JSONObject> it = inst.iterator();
		while (it.hasNext()) {
			sqlStr= (String) it.next().get("stmt");
			type= (String) it.next().get("type");
			System.out.println(sqlStr );

			sqlStr = parseStmt(sqlStr);  //replace place holders

			rslt = db.runDBcmd(sqlStr, type);
		}

		return true;
	}

	// TODO: move most code into DB as as part of registering table.
	private void initFieldMetaData() {
		Statement lrepStmt;
		ResultSet lrRset;
		int i;

		avroSchema = "{\"namespace\": \"com.future.DI2020.avro\", \n" 
				    + "\"type\": \"record\", \n" 
				    + "\"name\": \"" + tskDetailJSON.get("src_schema")+"."+ tskDetailJSON.get("src_table") + "\", \n" 
				    + "\"fields\": [ \n" ;
		
		try {
			lrepStmt = repConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

		i = 0;
		lrRset = lrepStmt.executeQuery(
			  "select src_field, src_field_type, tgt_field, java_type, avro_type from data_field "
			+ " where task_id=" + taskID + " order by field_id");

		//first line
		if (lrRset.next()) {
			//fldType[i] = lrRset.getInt("java_type");
			//fldNames[i] = lrRset.getString("src_field");
			fldType.add(lrRset.getInt("java_type"));
			fldNames.add(lrRset.getString("src_field"));

			avroSchema = avroSchema 
					+ "{\"name\": \"" + lrRset.getString("tgt_field") + "\", " + lrRset.getString("avro_type") + "} \n" ;
			i++;
		}
		//rest line (but not the last)
		while (lrRset.next() ) {   
			//if( lrRset.isLast()) {                                               //In DB2AS400, a.rrn(a) as DB2RRN is wrong syntaxly;
			//	if(tskDetailJSON.get("db_type").toString().contains("DB2/AS400")){  // but "a." is needed for Oracle.
			//	avroSchema = avroSchema 
			//			+ ", {\"name\": \"DB2RRN\", \"type\": " + lrRset.getString("avro_type") + "} \n" ;
			//	}if(tskDetailJSON.get("db_type").toString().contains("ORACLE")){
			//		avroSchema = avroSchema 
			//				+ ", {\"name\": \"ORARID\", \"type\": " + lrRset.getString("avro_type") + "} \n" ;
			//	}
			//	keyDataType = lrRset.getString("src_field_type");  //TODO: not a safe way to assume the last one is the PK!!
			//}else {
				keyDataType = lrRset.getString("src_field_type");  //TODO: not a safe way to assume the last one is the PK!!
				avroSchema = avroSchema 
						+ ", {\"name\": \"" + lrRset.getString("tgt_field") + "\", " + lrRset.getString("avro_type") + "} \n" ;
			//}
			fldType.add(lrRset.getInt("java_type"));
			fldNames.add(lrRset.getString("src_field"));
			i++;
			// System.out.println(i);
		}

		fldCnt=i;
		lrRset.close();
		lrepStmt.close();
		
		avroSchema = avroSchema 
				+ "] }";

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	//may not needed later on.
	public ArrayList<Integer> getFldJavaType() {
		return fldType;
	}
	public ArrayList<String> getFldNames() {
		return fldNames;
	}
	public String getSQLInsTgt() {
		//return sqlInsertTarget;
		return tskDetailJSON.get("tgt_stmt0").toString();
	}
	private JSONObject getAct1SQLs() {
		JSONObject jo = new JSONObject();
		JSONArray pre = new JSONArray();
		pre.add(1, getBareSrcSQL() );
		jo.put("PRE", pre);
		
		return jo;
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
			return Long.valueOf(tskDetailJSON.get("seq_last_ref").toString());
		}catch (NullPointerException e) {
			return -1;
		}
	}

	public int getPoolID() {
		return poolID;
	}


	public String getPK() {
		return tskDetailJSON.get("data_pk").toString();
	}

	public String getBareSrcSQL() {
		//return sqlSelectSource;
		return tskDetailJSON.get("src_stmt0").toString();
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
		return avroSchema;
	}
	
	/**** task admin APIs ****/
	public void setupTask(String jobId, Map<String, String> vars) {
		int rtc;
		
		jobID = jobId;
		actID = 11;    //regist a new task
		taskID = -1;   //taskID is to be generated.
		
		tskDetailJSON=new JSONObject(vars);
		
		jobId = jobId + tskDetailJSON.get("DISRCTBL");
	}

	public void disable() {
		DataPoint srcDB = dataMgr.getDB((String) (tskDetailJSON.get("DISRCDB")));
		DataPoint tgtDB = dataMgr.getDB((String) (tskDetailJSON.get("DITGTDB")));;

		JSONObject jsonRslt;
		JSONObject jo;

		//src side
		jo = (JSONObject) getDBlvlInstr((String) (tskDetailJSON.get("DISRCDB"))).get("isntru");
		JSONArray stmts = (JSONArray) jo.get("disable");
		runDBcmds(srcDB, stmts);
		//tgt side
		jo = (JSONObject) getDBlvlInstr((String) (tskDetailJSON.get("DITGTDB"))).get("instru");
		stmts = (JSONArray) jo.get("disable");
		runDBcmds(tgtDB, stmts);
		
		String sql = "update task set .... ";
		runUpdateSQL(sql);
	}

	public void unregist() {
		DataPoint srcDB = dataMgr.getDB((String) (tskDetailJSON.get("DISRCDB")));
		DataPoint tgtDB = dataMgr.getDB((String) (tskDetailJSON.get("DITGTDB")));;

		JSONObject jsonRslt;
		//src side
		JSONObject jo = (JSONObject) getDBlvlInstr((String) (tskDetailJSON.get("DISRCDB"))).get("instr");
		JSONArray stmts = (JSONArray) jo.get("unregist");
		runDBcmds(srcDB, stmts);
		//tgt side
		jo = (JSONObject) getDBlvlInstr((String) (tskDetailJSON.get("DITGTDB"))).get("instr");
		stmts = (JSONArray) jo.get("unregist");
		runDBcmds(tgtDB, stmts);
		
		String sql = "update task set ...";
		runUpdateSQL(sql);
	}

	public void regist() {
		preRegistCheck();

		DataPoint srcDB = dataMgr.getDB((String) (tskDetailJSON.get("DISRCDB")));
		DataPoint tgtDB = dataMgr.getDB((String) (tskDetailJSON.get("DITGTDB")));;

		//srcDB.setTable((String) tskDetailJSON.get("DISRCTBL"));
		//tgtDB.setTable((String) tskDetailJSON.get("DISRCTBL"));
		
		//src side
		JSONObject jsonRslt;
		JSONObject jo = (JSONObject) getDBlvlInstr((String) (tskDetailJSON.get("DISRCDB"))).get("instr");

		String bareSQL = (String) jo.get("bareSQL");
		String srcQRY = srcDB.getSrcSTMT(bareSQL);
		String tgtDDL = srcDB.getTgtDDL(bareSQL);
		String srcAVRO = srcDB.getAVRO(bareSQL);
		
		JSONArray stmts = (JSONArray) jo.get("regist");
		stmts = (JSONArray) jo.get("preCheck");
		runDBcmds(tgtDB, stmts);
		//tgt side
		jo = (JSONObject) getDBlvlInstr((String) (tskDetailJSON.get("DITGTDB"))).get("instr");
		stmts = (JSONArray) jo.get("regist");
		stmts = (JSONArray) jo.get("preCheck");
		runDBcmds(tgtDB, stmts);

		//insert into task
		java.sql.Timestamp ts = new java.sql.Timestamp(System.currentTimeMillis());
		String insTask = "insert into task (taskid, srcdb, srctbl, mbr_lst, tgtdb, tgttbl,regist_ts) " 
					+ " values( " + tskDetailJSON.get("DITASKID") + ", '" 
					+ tskDetailJSON.get("DISRDDBID") + "',  '" + tskDetailJSON.get("DISRCTBL") + "',  '" 
					+ tskDetailJSON.get("DITGTDBID") + "', '" + tskDetailJSON.get("TGTTBLID") + "', '" 
					+ ts +"')";
		runUpdateSQL(insTask);	
	
		//return DB PTRs to the db ptr mgr
		dataMgr.returnDB((String) (tskDetailJSON.get("DISRCDB")), srcDB);		
		dataMgr.returnDB((String) (tskDetailJSON.get("DITGTDB")), tgtDB);		
	}

	private String parseStmt(String sqlStr) {
		String sqlStmt = sqlStr;

		//String sqlStmt = "This is a TEST . another UPPER case";
		Pattern pattern = Pattern.compile("\\b[A-Z0-9]['A-Z0-9]+|\\b[A-Z]\\b");//, Pattern.CASE_INSENSITIVE);
		Matcher matcher = pattern.matcher(sqlStr);
		while ( matcher.find()) {
			//System.out.println("Found the text \"" + matcher.group()
			//+ "\" starting at " + matcher.start()
			//+ " index and ending at index " + matcher.end());
			sqlStmt.replaceAll(matcher.group(), (String) tskDetailJSON.get(tskDetailJSON));
		}
		
		return sqlStmt;
	}

	private boolean preRegistCheck() {
		String sql;
		JSONArray rslt;
		
		if(null!=tskDetailJSON.get("DITASKID")) {
			taskID = Integer.parseInt((String) tskDetailJSON.get("TASKID"));
		}else {
			taskID = getNextTaskID();
		}
		
		//verify taskID is not used
		sql = "select task_id from task where task_id = " + taskID ; 
		rslt = (JSONArray) SQLtoJSONArray(sql);
		if(rslt.size()>0) {
			logger.error("   task ID is already used!");
			return false;
		}
		//verify source name not registered 
		sql = "select task_id from task where SRC_DB_ID='" + tskDetailJSON.get("DISRCDBID") 
				+ "' and SRC_TBL='"	+ tskDetailJSON.get("DISRCTBL") 
				+ " and mbr_lst is null";  //if mbr_lst is not null, it is something like DB2/as400 journal ...
		rslt = (JSONArray) SQLtoJSONArray(sql);
		if(rslt.size()>0) {
			logger.error("   the source is already registered!");
			return false;
		}

		//pre-check DBs
		DataPoint srcDB = dataMgr.getDB((String) (tskDetailJSON.get("DISRCDB")));
		DataPoint tgtDB = dataMgr.getDB((String) (tskDetailJSON.get("DITGTDB")));

		JSONObject jo; 
		
		String sqlStr, type;
		JSONObject parm;
		//source side
		jo = (JSONObject) getDBlvlInstr((String) tskDetailJSON.get("DISRCDBID")).get("registration");
		JSONArray stmts = (JSONArray) jo.get("preCheck");
		runDBcmds(srcDB, stmts);
		//tgt side
		DataPoint tgtData = dataMgr.getDB((String) (tskDetailJSON.get("DITGTDB")));
		jo = (JSONObject)(JSONObject) getDBlvlInstr((String) tskDetailJSON.get("TGTDBID")).get("retistration");;
		stmts = (JSONArray) jo.get("preCheck");
		runDBcmds(tgtDB, stmts);

		//done with srdData and tgtData
		dataMgr.returnDB((String) (tskDetailJSON.get("DISRCDB")), srcDB);		
		dataMgr.returnDB((String) (tskDetailJSON.get("DITGTDB")), tgtDB);		

		return true;
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