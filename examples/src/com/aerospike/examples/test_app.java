package com.aerospike.examples;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.AerospikeException;

public class test_app {

    public static void main(String[] args) {
        AerospikeClient client = null;
        try {
            client = new AerospikeClient("localhost", 3101);

            Key key = new Key("test", "demo", "myKey123");

            Bin bin1 = new Bin("name", "Jane Doe");
            Bin bin2 = new Bin("age", 28);
            Bin bin3 = new Bin("city", "New York");

            client.put(null, key, bin1, bin2, bin3);
            System.out.println("Successfully written the record.");

            Record record = client.get(null, key);
            System.out.println("Read back the record.");

            if (record != null) {
                System.out.println("Record values are:");
                System.out.println("Name: " + record.getString("name"));
                System.out.println("Age: " + record.getInt("age"));
                System.out.println("City: " + record.getString("city"));
            } else {
                System.out.println("Record not found.");
            }

        } catch (AerospikeException e) {
            System.err.println("AerospikeException: " + e.getMessage());
        } finally {
            // 5. Close the client connection
            if (client != null) {
                client.close();
                System.out.println("Connection closed.");
            }
        }
    }
}