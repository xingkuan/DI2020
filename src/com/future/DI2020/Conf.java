package com.future.DI2020;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.PropertyResourceBundle;
import java.util.ResourceBundle;

public class Conf {

	static HashMap<String,String> confMap = new HashMap<String, String>();
	
    private static Conf instance = null;  // use lazy instantiation new OVSmetrix();
    
    private Conf(){}  //only to prevent clint from calling constructor.
    
    public static Conf getInstance() {
        if(instance == null) {
           instance = new Conf();
           init();
        }
        return instance;
     }
	
	public String getConf(String key){
		return confMap.get(key);
	}
	
	private static void init(){
	   InputStream input;
       try {
		input = new FileInputStream("config/config.properties");
		ResourceBundle resources = new PropertyResourceBundle(input);
		
		//convert ResourceBundle to Map
        Enumeration<String> keys = resources.getKeys();
        while (keys.hasMoreElements()) {
            String key = keys.nextElement();
            confMap.put(key, resources.getString(key));            
        }
    } catch (FileNotFoundException  ex) {
		 ex.printStackTrace();
	  } catch (IOException  ex) {
			 ex.printStackTrace();
		  }  
	} 
}
