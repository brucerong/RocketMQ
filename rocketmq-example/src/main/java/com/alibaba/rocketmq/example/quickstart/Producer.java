/**
 * Copyright (C) 2010-2013 Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.rocketmq.example.quickstart;

import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;


/**
 * Producer，发送消息
 * 
 */
public class Producer {
    public static void main(String[] args) throws MQClientException, InterruptedException {
        final DefaultMQProducer producer = new DefaultMQProducer("hahaha");
        producer.setNamesrvAddr("10.125.13.150:9876");
        producer.start();

        Thread[] threads = new Thread[5000];
        for(int i=0;i<2000;i++) {
        	Thread thread = new Thread(new Runnable() {
				
				@Override
				public void run() {
					while(true) {
						long time1 = System.currentTimeMillis();
			            
			            	for(int i=0;i<10;i++) {
			            		try {
			            			long timetime1 = System.currentTimeMillis();
				            		Message msg = new Message("guanghao",// topic
						                    "TagA",// tag
						                    ("Hello RocketMQ " + i).getBytes()// body
						                        );
//						            msg.setDelayTime(10000);
						            SendResult sendResult = producer.send(msg);
			            			long timetime2 = System.currentTimeMillis();
						            System.out.println((timetime2-timetime1)+"ms--"+sendResult);
			            		} catch (Exception e) {
					                e.printStackTrace();
					            }
			            	}
		            	long time2 = System.currentTimeMillis();
//			            if(time2-time1>1000) {
//				            	System.out.println(">1000");
//			            } else {
//			            	try {
//								Thread.sleep(1000-(time2-time1));
//							} catch (InterruptedException e) {
//								// TODO Auto-generated catch block
//								e.printStackTrace();
//							}
//			            }
			            
			        }
					
				}
				
			}, "thread"+i);
        	threads[i] = thread;
        }
        
        for(int i=0;i<5000;i++) {
        	threads[i].start();
        }
        
        Thread.sleep(1000*60);

        for(int i=0;i<5000;i++) {
        	threads[i].stop();
        }
        producer.shutdown();
    }
    
}
