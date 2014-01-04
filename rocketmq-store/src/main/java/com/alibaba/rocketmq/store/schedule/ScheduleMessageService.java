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
package com.alibaba.rocketmq.store.schedule;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.common.ConfigManager;
import com.alibaba.rocketmq.common.TopicFilterType;
import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.common.help.ScheduleHelper;
import com.alibaba.rocketmq.common.message.MessageConst;
import com.alibaba.rocketmq.common.message.MessageDecoder;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.ScheduleMsgInfo;
import com.alibaba.rocketmq.common.running.RunningStats;
import com.alibaba.rocketmq.store.ConsumeQueue;
import com.alibaba.rocketmq.store.DefaultMessageStore;
import com.alibaba.rocketmq.store.MessageExtBrokerInner;
import com.alibaba.rocketmq.store.PutMessageResult;
import com.alibaba.rocketmq.store.PutMessageStatus;
import com.alibaba.rocketmq.store.ScheduleConsumeQueue;
import com.alibaba.rocketmq.store.SelectMapedBufferResult;


/**
 * 定时消息服务
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-21
 */
public class ScheduleMessageService extends ConfigManager {
    public static final String SCHEDULE_TOPIC = "SCHEDULE_TOPIC_XXXX";
    public static final String PRECISE_SCHEDULE_TOPIC = "PRECISE_SCHEDULE_TOPIC_XXXX";
    public static final int SCHEDULE_QUEUE_ID = 100;
    private static final Logger log = LoggerFactory.getLogger(LoggerName.StoreLoggerName);
    private static final long FIRST_DELAY_TIME = 1000L;
    private static final long DELAY_FOR_A_WHILE = 100L;
    private static final long DELAY_FOR_A_PERIOD = 10000L;
    private static final long SCHEDULE_PERIOD = 200L;
    private static final long SCHEDULE_LOAD_STORAGE_PERIOD = 60*1000L; //一分钟一次查看未触发的定时
    private static final int PRECISE_SLOT_SIZE = 675;
    // 每个level对应的延时时间
    private final ConcurrentHashMap<Integer /* level */, Long/* delay timeMillis */> delayLevelTable =
            new ConcurrentHashMap<Integer, Long>(32);
    // 延时计算到了哪里
    private final ConcurrentHashMap<Integer /* level */, Long/* offset */> offsetTable =
            new ConcurrentHashMap<Integer, Long>(32);
    //准时队列到了哪里
    private final ConcurrentHashMap<Integer /* time */, Long/* offset */> preciseOffsetTable =
        new ConcurrentHashMap<Integer, Long>(144);
    // 内存存储
    private final ConcurrentHashMap<Long, List<ScheduleMsgInfo>> scheduleMsgTable = 
    		new ConcurrentHashMap<Long, List<ScheduleMsgInfo>>(32);
    // 定时器
    private final Timer timer = new Timer("ScheduleMessageTimerThread", true);
    // 存储顶层对象
    private final DefaultMessageStore defaultMessageStore;
    // 最大值
    private int maxDelayLevel;
    //已经处理完的时间点标记
    private long[] processTimeTag = new long[PRECISE_SLOT_SIZE];
    //处理到的时间点(非绝对准确时间点，数据恢复用)
    private long preciseTime;

    public ScheduleMessageService(final DefaultMessageStore defaultMessageStore) {
        this.defaultMessageStore = defaultMessageStore;
    }
    
    private void processPreciseTag(int queue, int slot) {
    	long secondOfDay = queue*60*10 + slot;
    	int arrSlot = (int)secondOfDay / PRECISE_SLOT_SIZE;
    	int bitSlot = 8 - (int)secondOfDay % PRECISE_SLOT_SIZE;
    	processTimeTag[arrSlot] = processTimeTag[arrSlot]|(1<<(bitSlot-1));
    	preciseTime = secondOfDay;
    }
    
    public void buildRunningStats(HashMap<String, String> stats) {
        Iterator<Entry<Integer, Long>> it = this.offsetTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<Integer, Long> next = it.next();
            int queueId = delayLevel2QueueId(next.getKey());
            long delayOffset = next.getValue();
            long maxOffset = this.defaultMessageStore.getMaxOffsetInQuque(SCHEDULE_TOPIC, queueId);
            String value = String.format("%d,%d", delayOffset, maxOffset);
            String key = String.format("%s_%d", RunningStats.scheduleMessageOffset.name(), next.getKey());
            stats.put(key, value);
        }
    }


    public static int queueId2DelayLevel(final int queueId) {
        return queueId + 1;
    }


    public static int delayLevel2QueueId(final int delayLevel) {
        return delayLevel - 1;
    }


    private void updateOffset(int delayLevel, long offset) {
        this.offsetTable.put(delayLevel, offset);
    }


    public long computeDeliverTimestamp(final int delayLevel, final long storeTimestamp) {
        Long time = this.delayLevelTable.get(delayLevel);
        if (time != null) {
            return time + storeTimestamp;
        }

        return storeTimestamp + 1000;
    }
    
    
    public long computePreciseDeliverTimestamp(final long deplayTime, final long storeTimestamp) {
        return storeTimestamp + deplayTime;
    }

    public long getPreciseOffset(int queueId) {
    	return preciseOffsetTable.get(queueId)==null?0:preciseOffsetTable.get(queueId);
    }

    public void start() {
        // 为每个延时队列增加定时器
        for (Integer level : this.delayLevelTable.keySet()) {
            Long timeDelay = this.delayLevelTable.get(level);
            Long offset = this.offsetTable.get(level);
            if (null == offset) {
                offset = 0L;
            }

            if (timeDelay != null) {
                this.timer.schedule(new DeliverDelayedMessageTimerTask(level, offset), FIRST_DELAY_TIME);
            }
        }
        
        //为准时队列增加定时器(10个定时器，防止1S内的消息处理时间大于1S的情况发生)
        int count = 10;
        while(count-->0) {
        	this.timer.scheduleAtFixedRate(new DeliverPreciseScheduleMessageTask(0, 0), 200, SCHEDULE_PERIOD);
        }
        
        //准时队列内存加载定时器(5分钟前开始加载内存)
        this.timer.scheduleAtFixedRate(new LoadStorageTask(), 200, SCHEDULE_LOAD_STORAGE_PERIOD);
        // 定时将延时进度刷盘
        this.timer.scheduleAtFixedRate(new TimerTask() {

            @Override
            public void run() {
                try {
                    ScheduleMessageService.this.persist();
                }
                catch (Exception e) {
                    log.error("scheduleAtFixedRate flush exception", e);
                }
            }
        }, 10000, this.defaultMessageStore.getMessageStoreConfig().getFlushDelayOffsetInterval());
    }


    public void shutdown() {
        this.timer.cancel();
    }


    public int getMaxDelayLevel() {
        return maxDelayLevel;
    }


    public String encode() {
        return this.encode(false);
    }


    public String encode(final boolean prettyFormat) {
        DelayOffsetSerializeWrapper delayOffsetSerializeWrapper = new DelayOffsetSerializeWrapper();
        delayOffsetSerializeWrapper.setOffsetTable(this.offsetTable);
        delayOffsetSerializeWrapper.setPreciseOffsetTable(this.preciseOffsetTable);
        delayOffsetSerializeWrapper.setPreciseTime(preciseTime);
        delayOffsetSerializeWrapper.setProcessTimeTag(processTimeTag);
        return delayOffsetSerializeWrapper.toJson(prettyFormat);
    }


    @Override
    public void decode(String jsonString) {
        if (jsonString != null) {
            DelayOffsetSerializeWrapper delayOffsetSerializeWrapper =
                    DelayOffsetSerializeWrapper.fromJson(jsonString, DelayOffsetSerializeWrapper.class);
            if (delayOffsetSerializeWrapper != null) {
                this.offsetTable.putAll(delayOffsetSerializeWrapper.getOffsetTable());
            }
        }
    }


    @Override
    public String configFilePath() {
        return this.defaultMessageStore.getMessageStoreConfig().getDelayOffsetStorePath();
    }


    public boolean load() {
        boolean result = super.load();
        int queueId = ScheduleHelper.getQueueId(System.currentTimeMillis());
        ScheduleConsumeQueue queue = (ScheduleConsumeQueue)defaultMessageStore.findConsumeQueue(PRECISE_SCHEDULE_TOPIC, queueId);
		if(queue.getStatus().equals(ScheduleConsumeQueue.UNLOAD)) {
			result = result&&queue.storageLoad();
		}
		ScheduleConsumeQueue queue2 = (ScheduleConsumeQueue)defaultMessageStore.findConsumeQueue(PRECISE_SCHEDULE_TOPIC, (queueId+1)%144);
		if(queue2.getStatus().equals(ScheduleConsumeQueue.UNLOAD)) {
			result = result&&queue2.storageLoad();
		}
        result = result && this.parseDelayLevel();
        return result;
    }


    public boolean parseDelayLevel() {
        HashMap<String, Long> timeUnitTable = new HashMap<String, Long>();
        timeUnitTable.put("s", 1000L);
        timeUnitTable.put("m", 1000L * 60);
        timeUnitTable.put("h", 1000L * 60 * 60);
        timeUnitTable.put("d", 1000L * 60 * 60 * 24);

        String levelString = this.defaultMessageStore.getMessageStoreConfig().getMessageDelayLevel();
        try {
            String[] levelArray = levelString.split(" ");
            for (int i = 0; i < levelArray.length; i++) {
                String value = levelArray[i];
                String ch = value.substring(value.length() - 1);
                Long tu = timeUnitTable.get(ch);

                int level = i + 1;
                if (level > this.maxDelayLevel) {
                    this.maxDelayLevel = level;
                }
                long num = Long.parseLong(value.substring(0, value.length() - 1));
                long delayTimeMillis = tu * num;
                this.delayLevelTable.put(level, delayTimeMillis);
            }
        }
        catch (Exception e) {
            log.error("parseDelayLevel exception", e);
            log.info("levelString String = {}", levelString);
            return false;
        }

        return true;
    }

    class LoadStorageTask extends TimerTask {
    	@Override
        public void run() {
            try {
                this.executeStorageLoader();
            }
            catch (Exception e) {
                log.error("executePreciseOnTimeup exception", e);
                ScheduleMessageService.this.timer.schedule(new LoadStorageTask(), DELAY_FOR_A_PERIOD);
            }
        }
    	
    	private void executeStorageLoader() {
    		int queueId = ScheduleHelper.getQueueId(System.currentTimeMillis()+5*60*1000);
    		ScheduleConsumeQueue queue = (ScheduleConsumeQueue)defaultMessageStore.findConsumeQueue(PRECISE_SCHEDULE_TOPIC, queueId);
    		if(queue.getStatus().equals(ScheduleConsumeQueue.UNLOAD)) {
    			boolean result = queue.storageLoad();
    			if(!result) {
                    ScheduleMessageService.this.timer.schedule(new LoadStorageTask(), DELAY_FOR_A_PERIOD);
    			}
    		}
    	}
    }
    
    class DeliverPreciseScheduleMessageTask extends TimerTask {

    	private int queueId;
    	
    	private int slot;
    	
    	public DeliverPreciseScheduleMessageTask(int queueId, int slot) {
    		this.queueId = queueId;
    		this.slot = slot;
        }

        @Override
        public void run() {
            try {
                this.executePreciseOnTimeup();
            }
            catch (Exception e) {
                log.error("executePreciseOnTimeup exception", e);
                ScheduleMessageService.this.timer.schedule(new DeliverPreciseScheduleMessageTask(queueId, slot), SCHEDULE_PERIOD);
            }
        }


        private void executePreciseOnTimeup() {
        	long timestamp = System.currentTimeMillis();
        	int queueId = ScheduleHelper.getQueueId(timestamp);
        	int slot = ScheduleHelper.getSlotInQueue(timestamp, queueId);
        	if(this.queueId!=0) {
        		queueId = this.queueId;
        		slot = this.slot;
        	}
        	ScheduleConsumeQueue queue = (ScheduleConsumeQueue)ScheduleMessageService.this.defaultMessageStore.findConsumeQueue(PRECISE_SCHEDULE_TOPIC, queueId);
        	if(queue!=null) {
        		if(!queue.getIsInProcess(slot).get()) {
        			boolean oldValue = queue.getIsInProcess(slot).getAndSet(true);
        			if(!oldValue) {
        				ConcurrentLinkedQueue<ScheduleMsgInfo> msgQueue = queue.getScheduleMsgs(slot);
                		ScheduleMsgInfo msg = msgQueue.poll();
                		while(msg!=null) {
                			long commitOffset = msg.getCommitOffset();
                			int size = msg.getSize();
                			MessageExt msgExt = ScheduleMessageService.this.defaultMessageStore.lookMessageByOffset(
                					commitOffset, size);
        	                if (msgExt != null) {
        	                    MessageExtBrokerInner msgInner = messageTimeup(msgExt);
        	                    PutMessageResult putMessageResult =
        	                            ScheduleMessageService.this.defaultMessageStore
        	                                .putMessage(msgInner);
        	                    boolean isSuccess = putMessageResult != null && putMessageResult.getPutMessageStatus() == PutMessageStatus.PUT_OK;
        	                    if(!isSuccess) {
        	                    	msgQueue.add(msg);
        	                    }
        	                }
                			msg = msgQueue.poll();
                		}
                		if(slot==599) {
                			long lastOffset = queue.releaseStorage();
                    		preciseOffsetTable.put(queue.getQueueId(), lastOffset);
                		}
        			}
        		}
        	}
        	
        }
        
    }
    

    
    class DeliverDelayedMessageTimerTask extends TimerTask {
        private final int delayLevel;
        private final long offset;


        public DeliverDelayedMessageTimerTask(int delayLevel, long offset) {
            this.delayLevel = delayLevel;
            this.offset = offset;
        }


        @Override
        public void run() {
            try {
                this.executeOnTimeup();
                
            }
            catch (Exception e) {
                log.error("executeOnTimeup exception", e);
                ScheduleMessageService.this.timer.schedule(new DeliverDelayedMessageTimerTask(
                    this.delayLevel, this.offset), DELAY_FOR_A_PERIOD);
            }
        }


        public void executeOnTimeup() {
            ConsumeQueue cq =
                    ScheduleMessageService.this.defaultMessageStore.findConsumeQueue(SCHEDULE_TOPIC,
                        delayLevel2QueueId(delayLevel));
            if (cq != null) {
                SelectMapedBufferResult bufferCQ = cq.getIndexBuffer(this.offset);
                if (bufferCQ != null) {
                    try {
                        long nextOffset = offset;
                        int i = 0;
                        for (; i < bufferCQ.getSize(); i += ConsumeQueue.CQStoreUnitSize) {
                            long offsetPy = bufferCQ.getByteBuffer().getLong();
                            int sizePy = bufferCQ.getByteBuffer().getInt();
                            long tagsCode = bufferCQ.getByteBuffer().getLong();

                            // 队列里存储的tagsCode实际是一个时间点
                            long deliverTimestamp = tagsCode;

                            nextOffset = offset + (i / ConsumeQueue.CQStoreUnitSize);

                            long countdown = deliverTimestamp - System.currentTimeMillis();
                            // 时间到了，该投递
                            if (countdown <= 0) {
                                MessageExt msgExt =
                                        ScheduleMessageService.this.defaultMessageStore.lookMessageByOffset(
                                            offsetPy, sizePy);
                                if (msgExt != null) {
                                    MessageExtBrokerInner msgInner = messageTimeup(msgExt);
                                    PutMessageResult putMessageResult =
                                            ScheduleMessageService.this.defaultMessageStore
                                                .putMessage(msgInner);
                                    // 成功
                                    if (putMessageResult != null
                                            && putMessageResult.getPutMessageStatus() == PutMessageStatus.PUT_OK) {
                                        continue;
                                    }
                                    // 失败
                                    else {
                                        log.error(
                                            "a message time up, but reput it failed, topic: {} msgId {}",
                                            msgExt.getTopic(), msgExt.getMsgId());
                                        ScheduleMessageService.this.timer.schedule(
                                            new DeliverDelayedMessageTimerTask(this.delayLevel, nextOffset),
                                            DELAY_FOR_A_PERIOD);
                                        ScheduleMessageService.this.updateOffset(this.delayLevel, nextOffset);
                                        return;
                                    }
                                }
                            }
                            // 时候未到，继续定时
                            else {
                                ScheduleMessageService.this.timer.schedule(
                                    new DeliverDelayedMessageTimerTask(this.delayLevel, nextOffset),
                                    countdown);
                                ScheduleMessageService.this.updateOffset(this.delayLevel, nextOffset);
                                return;
                            }
                        } // end of for

                        nextOffset = offset + (i / ConsumeQueue.CQStoreUnitSize);
                        ScheduleMessageService.this.timer.schedule(new DeliverDelayedMessageTimerTask(
                            this.delayLevel, nextOffset), DELAY_FOR_A_WHILE);
                        ScheduleMessageService.this.updateOffset(this.delayLevel, nextOffset);
                        return;
                    }
                    finally {
                        // 必须释放资源
                        bufferCQ.release();
                    }
                } // end of if (bufferCQ != null)
            } // end of if (cq != null)

            ScheduleMessageService.this.timer.schedule(new DeliverDelayedMessageTimerTask(this.delayLevel,
                this.offset), DELAY_FOR_A_WHILE);
        }

    }
    
    private MessageExtBrokerInner messageTimeup(MessageExt msgExt) {
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setBody(msgExt.getBody());
        msgInner.setFlag(msgExt.getFlag());
        msgInner.setProperties(msgExt.getProperties());

        TopicFilterType topicFilterType = MessageExt.parseTopicFilterType(msgInner.getSysFlag());
        long tagsCodeValue =
                MessageExtBrokerInner.tagsString2tagsCode(topicFilterType, msgInner.getTags());
        msgInner.setTagsCode(tagsCodeValue);
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgExt.getProperties()));

        msgInner.setSysFlag(msgExt.getSysFlag());
        msgInner.setBornTimestamp(msgExt.getBornTimestamp());
        msgInner.setBornHost(msgExt.getBornHost());
        msgInner.setStoreHost(msgExt.getStoreHost());
        msgInner.setReconsumeTimes(msgExt.getReconsumeTimes());

        msgInner.setWaitStoreMsgOK(false);
        msgInner.clearProperty(MessageConst.PROPERTY_DELAY_TIME_LEVEL);

        // 恢复Topic
        msgInner.setTopic(msgInner.getProperty(MessageConst.PROPERTY_REAL_TOPIC));

        // 恢复QueueId
        String queueIdStr = msgInner.getProperty(MessageConst.PROPERTY_REAL_QUEUE_ID);
        int queueId = Integer.parseInt(queueIdStr);
        msgInner.setQueueId(queueId);

        return msgInner;
    }
    
}