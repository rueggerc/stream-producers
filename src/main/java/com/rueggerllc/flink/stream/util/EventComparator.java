package com.rueggerllc.flink.stream.util;

import java.util.Comparator;

import com.rueggerllc.flink.stream.beans.EventBean;

public class EventComparator implements Comparator<EventBean> {

	@Override
	public int compare(EventBean event1, EventBean event2) {
		Integer processTime1 = event1.getProcessTimeOrder();
		Integer processTime2 = event2.getProcessTimeOrder();
		return processTime1.compareTo(processTime2);
	}

}
