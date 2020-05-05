package com.future.DI2020;


class Test
{
   private static final Metrix metrix = Metrix.getInstance();
   
   public static void main (String args[]) {   
	  metrix.sendMX("initDuration,jobId=test,tblID=0 value=6\n");
//	  metrix.sendMXrest("initDuration,jobId=test,tblID=0 value=6\n");
      
      return ;
   }

}