package com.riskcare.mongodb.demo;

import com.mongodb.client.FindIterable;
import org.bson.Document;

public class PrintOperations {

    public static void printAllPeopleRecords(DbOperations dbOps, String collectionName){
        FindIterable<Document> resultIter = dbOps.getCollection(collectionName).find();
        for(Document myDoc : resultIter){
            System.out.println("_id=" + myDoc.get("_id") + " user_id=" + myDoc.get("user_id") + " age=" + myDoc.get("age") + " first_name=" + myDoc.get("first_name")+ " last_name=" + myDoc.get("last_name"));
        }
    }

    public static void printCollectionNames(DbOperations dbOps){
        for (String collectionNames : DbOperations.getDatabase().listCollectionNames()) {
            System.out.println("collection name = " + collectionNames);
        }
    }

}
