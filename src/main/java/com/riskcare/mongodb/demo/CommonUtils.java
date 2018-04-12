package com.riskcare.mongodb.demo;

import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public class CommonUtils {

    public static Date makeDate(String str){
        //String str = "01/01/2015";
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy");
        LocalDate dateTime = LocalDate.parse(str, formatter);
        Date date = Date.from(dateTime.atStartOfDay(ZoneId.systemDefault()).toInstant());
        return  date;
        //System.out.println(dateTime.format(formatter)); // not using toString
    }

}
