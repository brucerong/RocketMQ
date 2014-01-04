package com.alibaba.rocketmq.common.help;

import java.util.Calendar;
import java.util.Date;

public class ScheduleHelper {

    public static long getTimeKey(long timestamp) {
    	Date date = new Date(timestamp);
    	Calendar c = Calendar.getInstance();
    	c.set(date.getYear()+1900, date.getMonth(), date.getDate(), date.getHours(), date.getMinutes(), date.getSeconds());
    	return c.getTimeInMillis();
    }
    
    public static int getQueueId(long timestamp) {
    	Calendar c = Calendar.getInstance();
    	c.setTimeInMillis(timestamp);
    	int hour = c.get(Calendar.HOUR_OF_DAY);
    	int minute = c.get(Calendar.MINUTE);
    	return (hour*60+minute)/10;
    }
}
