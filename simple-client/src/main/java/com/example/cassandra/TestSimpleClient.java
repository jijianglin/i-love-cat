package com.example.cassandra;

import java.util.concurrent.CountDownLatch;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class TestSimpleClient {

   public static void main(String[] args) throws InterruptedException {
	   long startTime = System.currentTimeMillis();
	   //int threadNum = 50;
	   // int perThreadNum = 10000;
	   int threadNum = Integer.parseInt(args[0]);
	   int perThreadNum = Integer.parseInt(args[1]);
	   System.out.println(threadNum);
	   System.out.println(perThreadNum);
	   CountDownLatch latch = new CountDownLatch(threadNum);
	   for(int i = 0; i < threadNum; i++){
		   new WriteThread(latch, perThreadNum).start();
	   }
	   latch.await();
	   long endTime = System.currentTimeMillis();
	   
	   System.out.println("TPS:" + threadNum*perThreadNum/((endTime-startTime)/1000));
	   
   }
}