package com.inin.analytics.elasticsearch.example;

import java.io.PrintWriter;
import java.util.UUID;

import org.apache.commons.lang.RandomStringUtils;

public class GenerateData {
	
	public static void main(String[] args) throws Exception {
		if(args.length != 2) {
			System.err.println("Invalid # arguments. EG: [num sample records] [output file]");
			return;
		}

		Long numRecords = new Long(args[0]);
		PrintWriter writer = new PrintWriter(args[1], "UTF-8");
		String attribute = null;
		String customer = null;
		String customer1 = UUID.randomUUID().toString();
		String customer2 = UUID.randomUUID().toString();

		for(Long x = 0l; x < numRecords; x++) {
			if(x % 2 == 0) {
				attribute = "yellow";
			} else {
				attribute = "blue";
			}
			
			if(x % 3 == 0) {
				customer = customer1;
			} else {
				customer = customer2;
			}
			
			writer.println(customer + "," + UUID.randomUUID().toString() + "," + attribute + "," + RandomStringUtils.randomAlphanumeric(15));
		}
		
		writer.close();
	}
}
