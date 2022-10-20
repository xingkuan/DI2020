package com.future.DI2020;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.stream.Collectors;

import org.json.simple.JSONObject;

import java.io.IOException;

public class taskAdmin {
	private static final TaskMeta taskMeta = TaskMeta.getInstance();
	private DataPointMgr dataMgr = DataPointMgr.getInstance();
	private static Entry e;

	
	public void main(String[] args) throws IOException {
		int actionId; //11: registration, 12: unregister; 13: disable;
		int taskId=0;
		String jobId;
		
		Map<String, String> vars=new HashMap<>();
		//e. g. vars["SRCDBID"]="ORASRC1"
		Map<String, String> exVars = null;
		
		//System.out.println(args.length);
		actionId = Integer.parseInt(args[0]);
		
		if(actionId==11) {
			if (args.length != 6) {
				System.out.println(
						"Usage:   taskAdmin actionId srdDb srcTbl tgtDb tgtTbl");
				return;
			} 
			taskId = Integer.parseInt(args[5]);
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
		
		switch (actionId) {
			case 11:	//registrating
				jobId = "regist ";
				taskId = -1;	//to be registered
				// if, e.g. datakeys needs pumped to kafka as DCC stage, then need a seperate task for that
				JSONObject jo = (JSONObject) taskMeta.getDBDetails(args[1]).get("instructions");
				String dccDtgDBID = (String) jo.get("dccFilter");
				if (dccDtgDBID != null) { //regist src to dcc first
					vars.put("DISRCDB", args[1]);
					vars.put("DISRCTBL", args[2]);
					vars.put("DITGTDB", dccDtgDBID);
					vars.put("DITGTTBL", exVars.get("DCTBL"));
					vars.put("DITASKID", String.valueOf(taskId));

					taskMeta.setupTask(jobId, vars);
					taskMeta.regist();
					
					taskId=taskId+1;
				}
				//then the regist src to tgt
				vars.put("DISRCDB", args[1]);
				vars.put("DISRCTBL", args[2]);
				vars.put("DITGTDB", args[3]);
				vars.put("DITGTTBL", args[4]);
				vars.put("DITASKID", args[5]);

				taskMeta.setupTask(jobId, vars);
				taskMeta.regist();
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
