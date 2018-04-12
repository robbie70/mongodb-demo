package com.riskcare.mongodb.demo;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import org.apache.commons.lang3.time.StopWatch;
import org.bson.Document;
import org.junit.Before;
import org.junit.Test;
import java.util.*;
import java.util.concurrent.TimeUnit;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static org.junit.Assert.assertEquals;

public class MapReduceTest {

    private static final int FIX_CUST = 3;
    private static final int TEST_PRICE = 5;
    private static final int TEST_ITEM_QTY_MMM = 5;
    private static final int TEST_ITEM_QTY_NNN = 10;
    private static final double TEST_ITEM_PRICE_MMM = 3;
    private static final double TEST_ITEM_PRICE_NNN = 5;
    private static final int NUM_TEST_RECORDS_TO_GENERATE = (3 * 5) * 1000 ;
    private static final DbOperations dbOps = new DbOperations();
    private static final StopWatch stopWatch = new StopWatch();
    MongoCollection<Document> collection = DbOperations.getDatabase().getCollection("orders");
    MapReduce mapReduce = new MapReduce();

    @Before
    public void setUp(){
        createLargeDataset();
    }

    @Test
    public void testLargeDataset(){
        assertEquals(collection.count(), NUM_TEST_RECORDS_TO_GENERATE -1);
    }

    @Test
    public void calcTotalPricePerCustomerTest(){
        StopWatch stopWatch = new StopWatch();
        String mapFunction = "function() {emit(this.cust_id, this.price);};";
        String reduceFunction = "function(keyCustId, valuesPrices) {return Array.sum(valuesPrices);};";


        stopWatch.start();
        mapReduce.calcTotalPricePerCustomer(collection, mapFunction, reduceFunction);
        stopWatch.stop();
        Document resultDoc = dbOps.findOne("map_reduce_example", and(eq("_id", "abc_3")));
        System.out.println("time taken for MapReduce step only in MS: " + stopWatch.getTime(TimeUnit.MILLISECONDS));
        System.out.println("_id: " + resultDoc.get("_id") + " value:" + resultDoc.get("value"));
        assertEquals("abc_3", resultDoc.get("_id"));
        assertEquals(TEST_PRICE * (getNumTestCustCreated() - 1), resultDoc.get("value"));
    }

    @Test
    public void calcOrderAndTotalQuantityWithAverageQuantityPerItemTest(){

        String mapFunction = "function() {\n" +
                "                       for (var idx = 0; idx < this.items.length; idx++) {\n" +
                "                           var key = this.items[idx].sku;\n" +
                "                           var value = {\n" +
                "                                         count: 1,\n" +
                "                                         qty: this.items[idx].qty\n" +
                "                                       };\n" +
                "                           emit(key, value);\n" +
                "                       }\n" +
                "                    };";
        String reduceFunction = "function(keySKU, countObjVals) {\n" +
                "                     reducedVal = { count: 0, qty: 0 };\n" +
                "\n" +
                "                     for (var idx = 0; idx < countObjVals.length; idx++) {\n" +
                "                         reducedVal.count += countObjVals[idx].count;\n" +
                "                         reducedVal.qty += countObjVals[idx].qty;\n" +
                "                     }\n" +
                "\n" +
                "                     return reducedVal;\n" +
                "                  };";
        String finalizeFunction = "function (key, reducedVal) {\n" +
                "\n" +
                "                       reducedVal.avg = reducedVal.qty/reducedVal.count;\n" +
                "\n" +
                "                       return reducedVal;\n" +
                "\n" +
                "                    };";
        stopWatch.reset();
        stopWatch.start();
        mapReduce.calcOrderAndTotalQuantityWithAverageQuantityPerItem(collection, mapFunction, reduceFunction, finalizeFunction);
        stopWatch.stop();
        System.out.println("time taken for MapReduce step only in MS: " + stopWatch.getTime(TimeUnit.MILLISECONDS));
        FindIterable<Document> resultDocs = dbOps.findRecords("map_reduce_example");
        for (Document resultDoc : resultDocs){
            if(resultDoc.get("_id").equals("mmm")){
                Document aggregateResults = (Document) resultDoc.get("value");
                System.out.println("mmm count: " + aggregateResults.get("count") + " qty:" + aggregateResults.get("qty") + " avg:" + aggregateResults.get("avg"));
                assertEquals(new Double(NUM_TEST_RECORDS_TO_GENERATE - 1), aggregateResults.get("count"));
                assertEquals(new Double((NUM_TEST_RECORDS_TO_GENERATE - 1)* TEST_ITEM_QTY_MMM), aggregateResults.get("qty"));
                Double avgResult = ((Double) aggregateResults.get("qty") / (Double) aggregateResults.get("count"));
                assertEquals(avgResult, aggregateResults.get("avg"));
            }
            if(resultDoc.get("_id").equals("nnn")){
                Document aggregateResults = (Document) resultDoc.get("value");
                System.out.println("nnn count: " + aggregateResults.get("count") + " qty:" + aggregateResults.get("qty") + " avg:" + aggregateResults.get("avg"));
                assertEquals(new Double(NUM_TEST_RECORDS_TO_GENERATE - 1), aggregateResults.get("count"));
                assertEquals(new Double((NUM_TEST_RECORDS_TO_GENERATE - 1)* TEST_ITEM_QTY_NNN), aggregateResults.get("qty"));
                Double avgResult = ((Double) aggregateResults.get("qty") / (Double) aggregateResults.get("count"));
                assertEquals(avgResult, aggregateResults.get("avg"));
            }
        }
    }

    public void createLargeDataset(){
        clearDownOrdersCollection();
        clearDownIntermediateCollections();
        List<Document> volumeDocs = new ArrayList<>();
        int custId;
        for(int i = 1; i < NUM_TEST_RECORDS_TO_GENERATE; i++) {
            Document doc = new Document("_id", i)
                    .append("cust_id", "abc_" + calcCustId(i))
                    .append("ord_date", calcDate(i))
                    .append("status", "A")
                    .append("price", TEST_PRICE)
                    .append("items", generateItems());
            volumeDocs.add(doc);
        }
        collection.insertMany(volumeDocs);
    }

    private double getNumTestCustCreated(){
        return NUM_TEST_RECORDS_TO_GENERATE / FIX_CUST;
    }

    private String calcCustId(int i) {
        if ((i % FIX_CUST) ==0){
            return String.valueOf(FIX_CUST);
        }else{
            return String.valueOf(i);
        }
    }

    private Date calcDate(int i){
        if ((i % FIX_CUST) ==0){
            return CommonUtils.makeDate("02/01/2012");
        }else{
            return CommonUtils.makeDate("02/01/2012");
            //return CommonUtils.makeDate("30/12/2011");
        }

    }

    private List generateItems(){
        List items = new ArrayList<>();
        Map inMap = new HashMap<>();
        inMap.put("sku", "mmm");
        inMap.put("qty", TEST_ITEM_QTY_MMM);
        inMap.put("price", TEST_ITEM_PRICE_MMM);
        items.add(inMap);
        Map inMap2 = new HashMap<>();
        inMap2.put("sku", "nnn");
        inMap2.put("qty", TEST_ITEM_QTY_NNN);
        inMap2.put("price", TEST_ITEM_PRICE_NNN);
        items.add(inMap2);
        return items;
        //System.out.println(items);
    }

    private void clearDownOrdersCollection(){
        MongoCollection<Document> collection = DbOperations.getDatabase().getCollection("orders");
        collection.drop();
    }

    private void clearDownIntermediateCollections(){
        MongoCollection<Document> collection = DbOperations.getDatabase().getCollection("map_reduce_example");
        collection.drop();
    }

}
