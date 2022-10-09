package com.future.DI2020;

import java.util.HashMap;
import java.util.Map;

import org.json.simple.JSONObject;

import java.io.IOException;

public class taskAdmin {
	private static final TaskMeta taskMeta = TaskMeta.getInstance();
	static DataPointMgr dataPtrManager = DataPointMgr.getInstance();

	public static void main(String[] args) throws IOException {
		int actionId; //11: registration, 12: unregister; 13: disable;
		int taskId=0;

		Map<String, String> vars=new HashMap<>();
		//e. g. vars["SRCDBID"]="ORASRC1"
		
		//System.out.println(args.length);
		actionId = Integer.parseInt(args[0]);
		
		if(actionId==11) {
			if (args.length != 6) {
				System.out.println(
						"Usage:   taskAdmin actionId srdDb srcTbl tgtDb tgtTbl");
				return;
			} 
		}else {
			if (args.length != 3) {
				System.out.println(
						"Usage:   taskAdmin actionId taskId");
				return;
			} 
			taskId = Integer.parseInt(args[2]);
		}
		
		switch (actionId) {
			case 11:
				// if, e.g. datakeys needs pumped to kafka as DCC stage, then need a seperate task for that
				JSONObject jo = (JSONObject) taskMeta.getDBDetails(args[1]).get("instructions");
				String dccInfo = (String) jo.get("dccFilter");
				if (dccInfo != null) { //regist src to dcc first
					vars.put("SRCDB", args[1]);
					vars.put("SRCTBL", args[2]);
					vars.put("TGTDB", "TODO");
					vars.put("TGTTBL", "TODO");
					vars.put("TASKID", "TODO");
					
					regist(vars);
				}
				//the regist src to tgt
				vars.put("SRCDB", args[1]);
				vars.put("SRCTBL", args[2]);
				vars.put("TGTDB", args[3]);
				vars.put("TGTTBL", args[4]);
				vars.put("TASKID", args[5]);

				regist(vars);
				break;
			case 12:
				unregist(taskId);
				break;
			case 13:
				disable(taskId);
				break;
			default:
				System.out.println("not a valid action.");
				break;
		}
	}
	
	private static void regist(Map<String, String> var) {
		taskMeta.regist(var);
	}
	private static void unregist(int taskId) {
		
	}
	private static void disable(int taskId) {
		
	}
}
