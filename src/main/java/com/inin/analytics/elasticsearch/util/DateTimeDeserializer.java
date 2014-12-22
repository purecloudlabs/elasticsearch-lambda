package com.inin.analytics.elasticsearch.util;

import java.lang.reflect.Type;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;

public class DateTimeDeserializer implements JsonDeserializer<DateTime> {
	public DateTime deserialize(JsonElement json, Type typeOfT,
			JsonDeserializationContext context) throws JsonParseException {
		JsonPrimitive primitive = json.getAsJsonPrimitive();
		if(primitive.isNumber()) {
			return new DateTime(primitive.getAsLong()).withZone(DateTimeZone.UTC);
		} else {
			return new DateTime(primitive.getAsString()).withZone(DateTimeZone.UTC);
		}
	}
}