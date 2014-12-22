package com.inin.analytics.elasticsearch.driver;

import java.util.TimeZone;

import org.apache.hadoop.util.ProgramDriver;
import org.joda.time.DateTimeZone;

import com.inin.analytics.elasticsearch.example.ExampleIndexingJob;
import com.inin.analytics.elasticsearch.example.ExampleJobPrep;
import com.inin.analytics.elasticsearch.example.GenerateData;

public class Driver extends ProgramDriver {

    public Driver() throws Throwable {
            super();
            addClass("generateExampleData", GenerateData.class, "Example job for how to build documents for elasticsearch indexing");
            addClass("examplePrep", ExampleJobPrep.class, "Example job for how to build documents for elasticsearch indexing");
            addClass("esIndexRebuildExample", ExampleIndexingJob.class, "Example job for how to rebuild elasticsearch indexes");
    }

    public static void main(String[] args) throws Throwable {   	
            DateTimeZone.setDefault(DateTimeZone.UTC);
            TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
            Driver driver = new Driver();
            driver.driver(args);
            System.exit(0);
    }
}
