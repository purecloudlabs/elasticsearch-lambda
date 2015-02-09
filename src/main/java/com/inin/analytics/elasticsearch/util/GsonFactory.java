package com.inin.analytics.elasticsearch.util;

import org.joda.time.DateTime;
import org.joda.time.LocalDate;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class GsonFactory {
    public static Gson buildGsonParser() {
        return buildGsonBuilder().create();
    }

    public static GsonBuilder buildGsonBuilder() {
        GsonBuilder builder = new GsonBuilder();
        builder.registerTypeAdapter(DateTime.class, new DateTimeSerializer());
        builder.registerTypeAdapter(DateTime.class, new DateTimeDeserializer());
        builder.registerTypeAdapter(LocalDate.class, new LocalDateSerializer());
        builder.registerTypeAdapter(LocalDate.class, new LocalDateDeserializer());
        return builder;
    }
}
