package com.riskcare.mongodb.demo;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.bson.conversions.Bson;

public class DbOperations {

    //static MongoClient mongoClient = new MongoClient("192.168.171.68", 27017);
    //static final MongoClient mongoClient = new MongoClient("localhost", 27017);
    private final static String DATABASE_NAME = "test";
    private static final MongoClient mongoClient = new MongoClient("localhost", 30000);
    private static final MongoDatabase database = mongoClient.getDatabase(DATABASE_NAME);

    public MongoCollection<Document> getCollection(String collectionName){
        MongoCollection<Document> collection = database.getCollection(collectionName);
        return collection;
    }

    public FindIterable<Document> findRecords(String collectionName){
        return getCollection(collectionName).find();
    }

    public FindIterable<Document> findRecords(String collectionName, Bson criteria){
        return getCollection(collectionName).find(criteria);
    }

    public Document findOne(String collectionName, Bson criteria){
        return getCollection(collectionName).find(criteria).first();
    }

    public static MongoDatabase getDatabase() {
        return database;
    }
}
