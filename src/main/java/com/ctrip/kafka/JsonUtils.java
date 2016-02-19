package com.ctrip.kafka;

import java.text.DateFormat;
import java.text.SimpleDateFormat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonUtils {

	private static ObjectMapper mapper = new ObjectMapper();;
	static {
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZZ");
		mapper.setDateFormat(df);
	}

	public static String toString(Object item) throws JsonProcessingException {
		return mapper.writeValueAsString(item);
	}
}
