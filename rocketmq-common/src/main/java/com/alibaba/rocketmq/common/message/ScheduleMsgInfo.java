package com.alibaba.rocketmq.common.message;

public class ScheduleMsgInfo {
	
	private long commitOffset;
	
	private int size;

	public long getCommitOffset() {
		return commitOffset;
	}

	public void setCommitOffset(long commitOffset) {
		this.commitOffset = commitOffset;
	}

	public int getSize() {
		return size;
	}

	public void setSize(int size) {
		this.size = size;
	}
	
}
