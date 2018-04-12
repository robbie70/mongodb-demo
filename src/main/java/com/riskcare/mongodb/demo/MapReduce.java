package com.riskcare.mongodb.demo;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import org.bson.BsonDocument;
import org.bson.BsonJavaScript;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.conversions.Bson;
import static com.mongodb.client.model.Filters.gte;

public class MapReduce {

    public void calcTotalPricePerCustomer(MongoCollection<Document> collection, String pMapFunction, String pReduceFunction){
        String mapFunction = pMapFunction;
        String reduceFunction = pReduceFunction;

        BsonDocument command = new BsonDocument();

        BsonJavaScript map = new BsonJavaScript(mapFunction);
        BsonJavaScript red = new BsonJavaScript(reduceFunction);
        BsonDocument outCollection = new BsonDocument("out", new BsonString("map_reduce_example"));
        //BsonDocument query = new BsonDocument("someidentifier", new BsonString("somevalue"));
        command.append("mapreduce", new BsonString("orders"));
        //command.append("query", query);
        command.append("map", map);
        command.append("reduce", red);
        //command.append("out", new BsonDocument("inline", new BsonBoolean(true)));
        //command.append("out", new BsonDocument("map_reduce_example", new BsonBoolean(true)));
        command.append("out", new BsonString("map_reduce_example"));

        Document result = DbOperations.getDatabase().runCommand(command);
    }

    public void calcOrderAndTotalQuantityWithAverageQuantityPerItem(MongoCollection<Document> collection, String pMapFunction, String pReduceFunction, String pFinalizeFunction){
        String mapFunction = pMapFunction;
        String reduceFunction = pReduceFunction;
        String finalizeFunction = pFinalizeFunction;

        BsonDocument command = new BsonDocument();
        BsonJavaScript map = new BsonJavaScript(mapFunction);
        BsonJavaScript red = new BsonJavaScript(reduceFunction);
        BsonJavaScript fin = new BsonJavaScript(finalizeFunction);

        Bson criteria = gte("ord_date", CommonUtils.makeDate("01/01/2012"));
        BsonDocument criteriaDocument = criteria.toBsonDocument(BsonDocument.class, MongoClient.getDefaultCodecRegistry());

        command.append("mapreduce", new BsonString("orders"));
        command.append("map", map);
        command.append("reduce", red);
        command.append("out", new BsonDocument("merge", new BsonString("map_reduce_example")));
        command.append("query", criteriaDocument);

        command.append("finalize", fin);

        Document result = DbOperations.getDatabase().runCommand(command);
    }


}
