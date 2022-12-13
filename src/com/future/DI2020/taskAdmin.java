package com.future.DI2020;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.stream.Collectors;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;

public class taskAdmin {
//	private static final TaskMeta taskMeta = TaskMeta.getInstance();
//	private DataPointMgr dataMgr = DataPointMgr.getInstance();
//	private static Entry e;

	
	public static void main(String[] args) throws IOException {
		int actionId; //11: registration, 12: unregister; 13: disable;
		int taskId=0;
		String jobId;
		
		//e. g. vars["SRCDBID"]="ORASRC1"
		Map<String, String> exVars = null;
		
		//System.out.println(args.length);
		actionId = Integer.parseInt(args[0]);
		
		if(actionId==11) {  //11 ORAD1 VERTSNAP.TEST VERTD1 TEST400.TEST
			if (args.length != 5) {
				System.out.println(
						"Usage:   taskAdmin actionId srdDb srcTbl tgtDb tgtTbl");
				return;
			} 
			//taskId = Integer.parseInt(args[5]); always generate  
	        Properties properties = System.getProperties();
	        
			// named parameter for, e.g, DB2/400 journal name; -DDIxxx=vvv
	        exVars = properties.entrySet()
	        		.stream()
	        		.filter(e ->  ((String) e.getKey()).startsWith("DI"))
	        		.collect(Collectors.toMap(
	        		        e -> (String) e.getKey(), 
	        		        e -> (String) e.getValue(),
	        		        (e1, e2) -> e2, 
	        		        LinkedHashMap::new));
	        exVars.forEach((k, v) -> System.out.println(k + ":" + v));
		}else {
			if (args.length != 3) {
				System.out.println(
						"Usage:   taskAdmin actionId taskId");
				return;
			} 
			taskId = Integer.parseInt(args[2]);
		}
		
		TaskMeta taskMeta = TaskMeta.getInstance();
		switch (actionId) {
			case 11:	//registrating
				jobId = "regist ";
				taskId = -1;	//to register
								//also, CDC dbid is to be retrieved from SRCDB

				Map<String, String> vars=new HashMap<>();
				Map<String, String> vars1=new HashMap<>();

				DBMeta repoDB = DBMeta.getInstance();
				//1st, get CDC river DB and "tbl name" from src DB info
				JSONObject jo = (JSONObject) repoDB.getDB(args[1]);
				String instr=(String) jo.get("instruct");
				JSONParser parser = new JSONParser();
				try {
					jo = (JSONObject) parser.parse(instr);
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				String cdcDBID = (String) jo.get("CDCDB");
				String cdcKEY = (String) jo.get("CCDKEY");
				String[] nameParts;
				//the CDC task
				{
					vars.put("DISRCDB", args[1]);
					nameParts = args[2].split("\\.", 2);
					vars.put("DISRCSCH", nameParts[0]);
					vars.put("DISRCTBL", nameParts[1]);
					vars.put("DITGTDB", cdcDBID);
					
					JSONObject cdcDBmeta = (JSONObject) repoDB.getDB(args[1]);
					String cdcDBengine = (String) cdcDBmeta.get("db_engine");
					//vars.put("DITGTTBL", exVars.get("DCTBL"));
					String cdcObjTemp = repoDB.getCDCNameTemp(cdcDBID, nameParts[1]);
					String cdcTbl = cdcObjTemp+"_LOG";
					String cdcTrg = cdcObjTemp+"_TRG";
					vars.put("CDCSCH", nameParts[0]);
					vars.put("CDCTBL", cdcTbl);
					vars.put("CDCTRG", cdcTrg);
					vars.put("DITGTTBL", cdcTbl);  //hack
					vars.put("DITASKID", String.valueOf(taskId));
					vars.put("DICURRST", "-1");
					vars.put("CDCKEY", cdcKEY);
					vars.put("DITGTDBEGIN", cdcDBengine);
				}
				//check all the way
				//1. cdc
				int tid;
				taskMeta.setupTask(jobId, vars);
				tid = taskMeta.preRegist();
				vars.put("DITASKID", String.valueOf(tid)); //put the real taskID in place. TODO not clean!
				if(tid<0) {
					System.out.println("cdc check failed!");
					return ;
				}
				//the src to tgt	
				{
					//taskId=tid+1;   //need to pass the previous taskId to here!
					vars1.put("DISRCDB", args[1]);
					nameParts = args[2].split(".", 2);
					vars1.put("DISRCSCH", nameParts[0]);
					vars1.put("DISRCTBL", nameParts[1]);
					vars1.put("DITGTDB", args[3]);
					
					JSONObject tgtDBmeta = (JSONObject) repoDB.getDB(args[1]);
					String tgtDBengine = (String) tgtDBmeta.get("db_engine");

					nameParts = args[4].split(".", 2);
					vars1.put("DITGTSCH", nameParts[0]);
					vars1.put("DITGTTBL", nameParts[1]);
					vars1.put("DICDCTASKID", String.valueOf(taskId));
					vars1.put("DITASKID", String.valueOf(taskId+1));
					vars1.put("DICURRST", "0");
					vars1.put("DITGTDBEGIN", tgtDBengine);
					//taskMeta.setupTask(jobId, vars1);
					//taskMeta.regist();
				}
				//2.data
				taskMeta.setupTask(jobId, vars1);
				tid = taskMeta.preRegist();
				if(tid<0) {
					System.out.println("cdc check failed!");
					return ;
				}
				//regist
				//1. cdc
				boolean isOk;
				taskMeta.setupTask(jobId, vars);
				isOk = taskMeta.regist();
				if(!isOk) {
					System.out.println("cdc regist failed!");
					return ;
				}
				//2.data
				taskMeta.setupTask(jobId, vars1);
				isOk = taskMeta.regist();
				if(!isOk) {
					System.out.println("cdc regist failed!");
					return ;
				}

				break;
			case 12:	//unregist
				jobId = "unregist ";
				taskMeta.setupTask(jobId, taskId, actionId);
				taskMeta.unregist();
				break;
			case 13:	//disable
				jobId = "disable ";
				taskMeta.setupTask(jobId, taskId, actionId);
				taskMeta.disable();
				break;
			default:
				System.out.println("not a valid action.");
				break;
		}
	}
	
}
