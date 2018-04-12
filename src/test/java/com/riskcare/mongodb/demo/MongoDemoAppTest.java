package com.riskcare.mongodb.demo;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.junit.Test;
import static com.mongodb.client.model.Filters.*;
import static com.riskcare.mongodb.demo.MongoDemoApp.COLLECTION_PEOPLE;
import static org.junit.Assert.assertEquals;

public class MongoDemoAppTest
{
    private static final DbOperations dbOps = new DbOperations();

    @Test
    public void checkNumberOfRecordsSameAsCreatedInShell()
    {
        MongoCollection<Document> myCollection = dbOps.getCollection(COLLECTION_PEOPLE);
        assertEquals(myCollection.count(), 4);
    }

    @Test
    public void checkNumberOfPeopleOfAgeGreaterThan25is3()
    {
        FindIterable<Document> resultIter = dbOps.findRecords(COLLECTION_PEOPLE, and(gt("age", 25)));
        int i = 0;
        for(Document myDoc : resultIter){
            System.out.println("_id=" + myDoc.get("_id") + " user_id=" + myDoc.get("user_id") + " age=" + myDoc.get("age") + " first_name=" + myDoc.get("first_name")+ " last_name=" + myDoc.get("last_name"));
            i++;
        }
        assertEquals(3, i);
    }
}
