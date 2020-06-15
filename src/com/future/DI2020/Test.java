package com.future.DI2020;

import org.json.simple.JSONObject;

class Test
{
   private static final Metrix metrix = Metrix.getInstance();
   
   public static void main (String args[]) {   
	   //metrix.sendMXrest("initDuration,jobId=test,tblID=0 value=6\n");
	   //metrix.sendMX("initDuration,jobId=test,tblID=0 value=6\n");
      
	   testAVROConsumer();
	   
	   return ;
   }
   
   private static void testAVROConsumer() {
		int tableID=6;
		MetaData metaData = MetaData.getInstance();

		metaData.setupTableForAction("testConsumeAVRO", tableID, 21);  // actId for dev activities.
		
		JSONObject tblDetail = metaData.getTableDetails();
		String actTemp = tblDetail.get("temp_id").toString();

		KafkaData tgtData = (KafkaData) DataPointer.dataPtrCreater(tblDetail.get("tgt_db_id").toString(), "TGT");
		tgtData.testConsumer();

   }

}