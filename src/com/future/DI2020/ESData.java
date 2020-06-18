package com.future.DI2020;

import java.io.*;
import java.util.*;
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
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.concurrent.Cancellable;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;

class ESData extends DataPointer{
	
   //public OracleData(String dbID) throws SQLException {
   public ESData(JSONObject dbID, String role) throws SQLException {
		super(dbID, role);
   }
   public ESData() {
	   System.out.println("used for testing only");
   }
   protected void initializeFrom(DataPointer dt) {
		ovLogger.info("   not needed yet");
   }
   public  void test() {
	   //http://dbatool02:9200
	   //https://www.elastic.co/guide/en/elasticsearch/client/java-rest/current/java-rest-low-usage-requests.html
	   RestClient restClient = RestClient.builder(
		       //new HttpHost("localhost", 9200, "http"),
		       new HttpHost("dbatool02", 9200, "http")).build();
	   Request request = new Request(
			    "GET",  
			    "/");   
	   Response response;
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

/*
	org.elasticsearch.client.Cancellable cancellable = restClient.performRequestAsync(request,
		    new ResponseListener() {
		        @Override
		        public void onSuccess(Response response) {
		            
		        }

		        @Override
		        public void onFailure(Exception exception) {
		            
		        }
		});
	*/
	
	//request.addParameter("pretty", "true");
	
	/*
	  request.setEntity(new NStringEntity(
        "{\"json\":\"text\"}",
        ContentType.APPLICATION_JSON));
	 */
	   /*HttpEntity entity = new NStringEntity(
			   "{\n" +
			   "    \"company\" : \"qbox\",\n" +                                      
			   "    \"title\" : \"Elasticsearch rest client\"\n" +
			   "}", ContentType.APPLICATION_JSON);
	   Response indexResponse = restClient.performRequest(
			   "PUT",
			   "/blog/post/1",
			   Collections.<String, String>emptyMap(),
			   entity);
			   */
	   //Response response = restClient.performRequest(
		//	   "GET", 
		//	   "/blog/_search",
		//	   Collections.singletonMap("pretty", "true"),
        //       entity1);
	   //restClient.performRequestAsync("POST", "/test" + "/_refresh");
	   //restClient.performRequest("POST", "test", Collections.emptyMap(), entity);
	   
	   
	}
   
	public boolean miscPrep(String jTemp) {
		boolean rtc=true;
		super.miscPrep(jTemp);
		//if(jTemp.equals("DJ2K")) { 
		//	rtc=initThisRefreshSeq();
		//}
		return rtc;
	}



   public void commit() {
      try {
		dbConn.commit();
	} catch (SQLException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
   }
   public void rollback()  {
      try {
		dbConn.rollback();
	} catch (SQLException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
   }

}