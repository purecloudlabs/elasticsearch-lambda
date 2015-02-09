package com.inin.analytics.elasticsearch.util;

import java.lang.reflect.Type;


import org.joda.time.LocalDate;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;

public class LocalDateDeserializer implements JsonDeserializer<LocalDate> {
	public LocalDate deserialize(JsonElement json, Type typeOfT,
			JsonDeserializationContext context) throws JsonParseException {
		JsonPrimitive primitive = json.getAsJsonPrimitive();
		return new LocalDate(primitive.getAsString());
	}
}