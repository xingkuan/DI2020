package com.future.DI2020;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;
import java.util.concurrent.CountDownLatch;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import java.text.*;
import java.sql.*;
import oracle.jdbc.*;
import oracle.jdbc.pool.OracleDataSource;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseListener;
import org.elasticsearch.client.RestClient;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.apache.avro.generic.GenericRecord;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpStatus;
import org.apache.http.concurrent.Cancellable;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;

class ESData extends DataPoint{
	RestClient restClient;
	
   //public OracleData(String dbID) throws SQLException {
   public ESData(JSONObject dbID) {
		super(dbID);
		//connect();
   }
   public ESData() {
	   System.out.println("this constructor is used for testing only");
   }
   @Override
	public boolean miscPrep() {
		boolean rtc=true;
		super.miscPrep();

		String jTemp=metaData.getActDetails().get("act_id").toString()+metaData.getActDetails().get("template_id"); 
		//if(jTemp.equals("DJ2K")) { 
		//	rtc=initThisRefreshSeq();
		//}
		return rtc;
	}
/********** sync APIs *******************/
	Request request;
	StringBuilder bulkRequestBody;
	String ixName; //="testok";
	public void setupSink() {
		connect();
		
		ixName = metaData.getTaskDetails().get("tgt_table").toString();
		request = new Request(
				   "POST", 
		           "/"+ixName+"/_bulk");
		/*action_and_meta_data\n
		 *optional_source\n
		 *...
		 *  { "index" : { "_index" : "myindex", "_id" : "1" } }
		 *  { "field1" : "value1" }
		 *  ...
		 */
		bulkRequestBody  = new StringBuilder();
	}
	@Override
	public void write(ResultSet rs) {
    	bulkRequestBody.append("{\"index\": {}}");  // automcatic ID ?
    	bulkRequestBody.append("\n");
    	bulkRequestBody.append(rs.toString());   //mem is json, in single line.
    	bulkRequestBody.append("\n");
	}
    @Override
	public void write(JSONObject rec) {
    	System.out.println(rec.toString());
    	String pk=metaData.getTaskDetails().get("data_pk").toString();
    	String docId = rec.get(pk).toString();
    	String ixStr ="{\"index\" : {\"_index\" :\"" 
    			+ ixName + "\", \"_id\" : \"" + docId + "\" } }";
	   	bulkRequestBody.append("{\"index\": {}}");  // automcatic ID ?
	   	bulkRequestBody.append("\n");
	   	bulkRequestBody.append(rec);   //mem is json, in single line.
	   	bulkRequestBody.append("\n");
    }
    @Override
	public void write(GenericRecord rec) {
    	System.out.println(rec.toString());
    	
    	String pk=metaData.getTaskDetails().get("data_pk").toString();
    	String docId = rec.get(pk).toString();
    	String ixStr ="{\"index\" : {\"_index\" :\"" 
    			+ ixName + "\", \"_id\" : \"" + docId + "\" } }";
	   	bulkRequestBody.append("{\"index\": {}}");  // automcatic ID ?
	   	bulkRequestBody.append("\n");
	   	bulkRequestBody.append(rec);   //mem is json, in single line.
	   	bulkRequestBody.append("\n");
	   	
    }
    @Override
	public void write() {
	   Request request = new Request(
			   "POST", 
	           "/" + ixName+"/_bulk");
	    request.setEntity(new NStringEntity(bulkRequestBody.toString(), ContentType.APPLICATION_JSON));
	    try {
			Response indexResponse = restClient.performRequest(request);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
/*****************************/
   protected void initializeFrom(DataPointMgr dt) {
		logger.info("   not needed yet");
   }
   private void connect() {
		String[] urls = urlString.split(",");
		HttpHost[] hosts = new HttpHost[urls.length];
		int i=0;
		try {
		    for (String address : urls) {
		        URL url;
					url = new URL(address);
		        hosts[i] = new HttpHost(url.getHost(), url.getPort(), url.getProtocol());
		        i++;
		    }
		
		    restClient = RestClient.builder(
		       //new HttpHost("dbatool02", 9200, "http"))
		    	hosts)
		    	.build();
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
   }
   @Override
   public void close() {
	   try {
		restClient.close();
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
   }
   public  void test() {
	   //http://dbatool02:9200,http://dbatool0a:9200
	   //https://www.elastic.co/guide/en/elasticsearch/client/java-rest/current/java-rest-low-usage-requests.html
	   Response response;

		Request request = new Request(
			    "GET",  
			    "/");   
	   try {
		   response = restClient.performRequest(request);
		   System.out.println(response.toString());
		   
		   System.out.println(EntityUtils.toString(response.getEntity()));
		   System.out.println("Host -" + response.getHost() );
		   System.out.println("RequestLine -"+ response.getRequestLine() );

		   restClient.close();
	   } catch (IOException e) {
		// TODO Auto-generated catch block
		   e.printStackTrace();
	   }
   } 
   public void testInsAdoc() {
	   HttpEntity entity = new NStringEntity(
			   "{\"company\" : \"qbox\",\n" +                                      
			   "    \"title\" : \"Elasticsearch rest client\"\n" +
			   "}", ContentType.APPLICATION_JSON);
	   Request request = new Request(
			   "PUT", 
	           "/myindex/_doc/1");
	     request.setEntity(entity);
	     
	   try {
		Response indexResponse = restClient.performRequest(request);
	   } catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	   }
	}
	public int syncDataFrom(DataPointMgr srcData) {
		List<String> docList = srcData.getDCCKeyList();
		try {
			bulkIndex(docList);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return 100;
	}
   public void bulkIndex(List<String> memList) throws Exception { 
	   Request request = new Request(
			   "POST", 
	           "/myindex/_bulk");
	   /*action_and_meta_data\n
	    *optional_source\n
	    *...
	    *  { "index" : { "_index" : "myindex", "_id" : "1" } }
		*  { "field1" : "value1" }
		*  ...
	    */
	   StringBuilder bulkRequestBody  = new StringBuilder();
	    for (Object mem : memList) {
	    	//bulkRequestBody.append("{\"index\": {\"_id\": \"" + docID + "\"}}");
	    	bulkRequestBody.append("{\"index\": {}}");  // automcatic ID ?
	    	bulkRequestBody.append("\n");
	    	bulkRequestBody.append(mem);   //mem is json, in single line.
	    	bulkRequestBody.append("\n");
	    }

	    request.setEntity(new NStringEntity(bulkRequestBody.toString(), ContentType.APPLICATION_JSON));

	    Response indexResponse = restClient.performRequest(request);

	}

   public void asynchIndex(List<String> items) {
	   CountDownLatch latch = new CountDownLatch(items.size());
	   ResponseListener listener = new ResponseListener() {
	     @Override
	     public void onSuccess(Response response) {
	       latch.countDown();
	     }
	     @Override
	     public void onFailure(Exception exception) {
	       latch.countDown();
	       logger.error("Could not process ES request. ", exception);
	     }
	   };
	       
	   items.stream().forEach(e-> {
	     Request request = new Request(
	                  "PUT", 
	                  String.format("/%s/_doc/%d", 
	                                "index", 2));
	       request.setJsonEntity(e);
		   restClient.performRequestAsync(request, listener);
	     });
	   try {
	     latch.await(); //wait for all the threads to finish
	     logger.info("Done inserting all the records to the index");
	   } catch (InterruptedException e1) {
	     logger.warn("Got interrupted.",e1);
	   }
	 }
   public void tobeTried() {
		//request.addParameter("pretty", "true");
		
		/*
		  request.setEntity(new NStringEntity(
	        "{\"json\":\"text\"}",
	        ContentType.APPLICATION_JSON));
		 */

   }
   public void process(JSONObject a) {
	   ////////////
	   //https://www.programcreek.com/java-api-examples/index.php?api=org.elasticsearch.client.Response
	   {
		   //RestClient restClient = ...;
		   String index="index1";
		   String type="index1Tupe";		   
		   String actionMetaData = String.format("{ \"index\" : { \"_index\" : \"%s\", \"_type\" : \"%s\" } }%n", 
				   index, type);

		   List<String> bulkData= List.of("{foo}", "{bar}", "{baz}");;
		   StringBuilder bulkRequestBody = new StringBuilder();
		   for (String bulkItem : bulkData) {
		       bulkRequestBody.append(actionMetaData);
		       bulkRequestBody.append(bulkItem);
		       bulkRequestBody.append("\n");
		   }
	/*	   
		   HttpEntity entity = new NStringEntity(bulkRequestBody.toString(), ContentType.APPLICATION_JSON);
		   try {
		       Response response = restClient.performRequest("POST", 
		    		   "/your_index/your_type/_bulk", 
		    		   Collections.<String, String>emptyMap(), 
		    		   entity);
		       return response.getStatusLine().getStatusCode() == HttpStatus.SC_OK;
		   } catch (Exception e) {
		       // do something
		   }
		   */
	   }
	   ////////////
	   
   }


   //Transform input JSON into the desired JSON to be inserted into ES
	private void transform() {
		    ScriptEngine graalEngine = new ScriptEngineManager().getEngineByName("graal.js");
		    try {
				graalEngine.eval("print('Hello Graal World!');");

				graalEngine.eval("function sum(a,b){return a.concat(b);}");
			    String v = (String)graalEngine.eval("sum(\"Hello, \", \"the other world!\")");
			    System.out.println(v);
		    } catch (ScriptException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}   
		}



   public void commit() {
   }
   public void rollback() {
   }

   
	/******** Registration APIs **********/
	@Override
	public boolean regSrcCheck(int tblID, String PK, String srcSch, String srcTbl, String dccPgm, String jurl, String tgtSch, String tgtTbl, String dccDBid) {
		//do nothing for Oracle trig based.
		return true;
	}
	@Override
	public boolean regSrc(int tblID, String PK, String srcSch, String srcTbl, String dccPgm, String jurl, String tgtSch, String tgtTbl, String dccDBid) {
		//not Vertica is not used as src so far.
		return false;
	}
	@Override
	public boolean regSrcDcc(int tblID, String PK, String srcSch, String srcTbl, String dccPgm, String jurl, String tgtSch, String tgtTbl, String dccDBid) {
		//not Vertica is not used as src so far.
		return false;
	}
	@Override
	public boolean regTgt(int tblID, String PK, String srcSch, String srcTbl, String dccPgm, String jurl, String tgtSch, String tgtTbl, String dccDBid) {
	/*
	 * 
PUT /test
{
 "settings" : {
    "number_of_shards" : 1
    "number_of_replicas" : 2
  },
  "mappings" : {
    "properties" : {
      "field1" : { "type" : "text" }
    }
  }
}
	 */
		String indxName = tgtSch+tgtTbl;
		connect();
		Response response;

		Request request = new Request(
			    "PUT",  
			    "/"+indxName);   
		
		String mappingStr="{\n" + 
				" \"settings\" : {\n" + 
				"    \"number_of_shards\" : 1\n" + 
				"    \"number_of_replicas\" : 2\n" + 
				"  },\n" + 
				"  \"mappings\" : {\n" + 
				"    \"properties\" : {\n" + 
				"      \"field1\" : { \"type\" : \"text\" }\n" + 
				"    }\n" + 
				"  }\n" + 
				"}\n" ;
		
		   HttpEntity entity = new NStringEntity(
				   mappingStr, ContentType.APPLICATION_JSON);

		     request.setEntity(entity);

		
		   try {
			   response = restClient.performRequest(request);
			   System.out.println(response.toString());
			   
			   System.out.println(EntityUtils.toString(response.getEntity()));
			   System.out.println("Host -" + response.getHost() );
			   System.out.println("RequestLine -"+ response.getRequestLine() );

			   restClient.close();
		   } catch (IOException e) {
			// TODO Auto-generated catch block
			   e.printStackTrace();
		   }
	
		
		return true;
	}
	/***************************************************/

}