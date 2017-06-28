package com.mapr.db.json;

import com.mapr.db.MapRDB;
import com.mapr.db.Table;
import org.ojai.Document;
import org.ojai.DocumentStream;

import java.util.Iterator;

public class RowCounterJava {

  public static void main(String[] args) {

    long start = System.currentTimeMillis();

    Table table = MapRDB.getTable(args[0]);
    DocumentStream rs = table.find("_id");
    Iterator<Document> itrs = rs.iterator();
    Document document;
    int totalRow = 0;
    while (itrs.hasNext()) {
      itrs.next();
      totalRow++;
    }
    rs.close();


    long end = System.currentTimeMillis();


    System.out.println("\n\n******************\n\nTotal document : "+ totalRow + " (in "+ (end-start) +" ms)\n\n");



  }


}
