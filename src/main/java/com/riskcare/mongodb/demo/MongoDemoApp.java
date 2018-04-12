package com.riskcare.mongodb.demo;

public class MongoDemoApp{

    public static final String COLLECTION_PEOPLE = "people";
    public static final String COLLECTION_ORDERS = "orders";
    private static final DbOperations dbOps = new DbOperations();

    public static void main( String[] args )
    {
        PrintOperations.printCollectionNames(dbOps);
        PrintOperations.printAllPeopleRecords(dbOps, COLLECTION_PEOPLE);
    }

}