package com.example.cassandra;

import java.util.concurrent.CountDownLatch;

public class WriteThread extends Thread {
	private CountDownLatch latch;
	private SimpleClient client;
	int num; // 

	public WriteThread(CountDownLatch latch , int num) {
		super();
		this.latch = latch;
		client = new SimpleClient();
		this.num = num;
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		super.run();
		client.connect("10.0.1.186");
	    client.createSchema();
	    for(int i = 0; i < num; i++)
	    client.loadData();
	    //client.querySchema();
	    client.close();
	    latch.countDown();		
	}
	
}
