package com.future.DI2020;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;

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
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.concurrent.Cancellable;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;

class ESData extends DataPoint{
	RestClient restClient;
	
   //public OracleData(String dbID) throws SQLException {
   public ESData(JSONObject dbID, String role) {
		super(dbID, role);
		connect();
   }
   public ESData() {
	   System.out.println("this constructor is used for testing only");
   }
	public boolean miscPrep(String jTemp) {
		boolean rtc=true;
		super.miscPrep(jTemp);
		//if(jTemp.equals("DJ2K")) { 
		//	rtc=initThisRefreshSeq();
		//}
		return rtc;
	}
   protected void initializeFrom(DataPoint dt) {
		ovLogger.info("   not needed yet");
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
   public void injectFrom(DataPoint kafkaData) {
	   for each data from src:
	      transform(it)
	      insertIntoES
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

}