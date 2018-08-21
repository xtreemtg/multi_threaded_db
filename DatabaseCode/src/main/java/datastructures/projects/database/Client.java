package datastructures.projects.database;
/*
 * This is the Client class, which sends queries to the API server and then prints out the response.
 * I have a bunch of queries and have the same query send twice at the same time (hence the two threads) to test that it works.
 * idk if this is the right way to test it or not
 */
import com.sun.deploy.net.URLEncoder;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Scanner;

public class Client {

    public static void main(String[] args) {
        ArrayList<String> list = new ArrayList<String>();
        list.add("CREATE TABLE YCStudent( BannerID int, SSNum int, FirstName varchar(255), LastName varchar(255) NOT NULL, GPA decimal(1,2) DEFAULT 0.01, CurrentStudent boolean DEFAULT true, Class varchar(255), PRIMARY KEY (BannerID) );");
        list.add("INSERT INTO YCStudent (FirstName, LastName, GPA, BannerID, CurrentStudent, SSNum) VALUES ('Noah','Potash', 4.0 ,80002, true, 40);");
        list.add("INSERT INTO YCStudent (FirstName, LastName, Class, GPA, BannerID, SSNum) VALUES ('Yonah', 'Taurog', 'Senior' ,3.9 ,80001 , 38);");
        list.add("INSERT INTO YCStudent (FirstName, LastName, GPA, BannerID) VALUES ('Basia', 'Ebel', 4.07, 8000909);");
        list.add("INSERT INTO YCStudent (FirstName, LastName, GPA, SSNum, BannerID) VALUES ('Gavriel', 'Baron', 2.9, 809, 04659234);");
        list.add("CREATE INDEX SSNum_Index on YCStudent (SSNum);");
        list.add("SELECT * FROM YCStudent WHERE GPA < 4.0 OR FirstName = 'Basia'");
        list.add("SELECT * FROM YCStudent;");
        list.add("SELECT GPA, LastName FROM YCStudent;");
        list.add("SELECT * FROM YCStudent ORDER BY GPA DESC, FirstName ASC, SSNum DESC;");
        list.add("SELECT COUNT (DISTINCT FirstName) FROM YCStudent;");
        list.add("SELECT SUM (GPA) FROM YCStudent;");
        list.add("SELECT AVG (GPA) FROM YCStudent;");
        list.add("SELECT MAX (BannerID) FROM YCStudent;");
        list.add("SELECT FirstName, LastName, SSNum from YCStudent WHERE GPA = 3.9 OR FirstName = 'Noah' AND SSNum > 37;");
        list.add("UPDATE YCStudent SET Class = 'Super Senior' WHERE SSNum < 809;");
        list.add("DELETE FROM YCStudent WHERE GPA < 4.0 AND FirstName = 'Yonah'");
        list.add("SELECT * FROM YCStudent;");

        ArrayList<String> list2 = new ArrayList<>();
        list2.add("INSERT INTO YCStudent (FirstName, LastName, GPA, BannerID, CurrentStudent, SSNum) VALUES ('Chad','Boniuk', 3.6 ,78002, true, 49);");
        list2.add("INSERT INTO YCStudent (FirstName, LastName, Class, GPA, BannerID, SSNum) VALUES ('Jonah', 'Taurog', 'Senior' , 3.3 ,80009 , 100);");
        list2.add("INSERT INTO YCStudent (FirstName, LastName, GPA, BannerID) VALUES ('Elana', 'Muller', 3.0, 1000909);");
        list2.add("INSERT INTO YCStudent (FirstName, LastName, GPA, SSNum, BannerID) VALUES ('Noah', 'Weiss', 3.1, 54, 04659235);");
        list2.add("SELECT * FROM YCStudent;");
        list2.add("SELECT GPA, LastName FROM YCStudent;");
        list2.add("SELECT * FROM YCStudent ORDER BY GPA ASC, FirstName ASC, SSNum DESC;");
        list2.add("SELECT COUNT (DISTINCT FirstName) FROM YCStudent;");
        list2.add("SELECT SUM (GPA) FROM YCStudent;");
        list2.add("SELECT AVG (SSNum) FROM YCStudent;");
        list2.add("SELECT MAX (BannerID) FROM YCStudent;");
        list2.add("SELECT FirstName, LastName, SSNum from YCStudent WHERE GPA = 3.3 OR FirstName = 'Noah' AND SSNum > 37;");
        //list2.add("DELETE FROM YCStudent WHERE SSum > 89 AND FirstName = 'Elana'");
        list2.add("SELECT * FROM YCStudent;");

//        for(int i = 0; i < list.size(); i++){
//            System.out.println(i + " CURRENTNUMBER");
//            String query = list.get(i);
//            try {
//                 if(query.matches("(SELECT|INSERT|CREATE|UPDATE|DELETE).*")){
//                     sendGET("http://localhost:8001/query?q=" + URLEncoder.encode(query, "UTF-8"));
//                } else{
//                    throw new IllegalArgumentException("Can't get SUM of empty column!");
//                }
//
//
//
//            }catch (IOException e) {
//                e.printStackTrace();
//            }
//
//
//        }


        Thread t1 = new Thread(new Runnable() {
            @Override
            public void run() {
                for(int i = 0; i < list.size(); i++){
                    System.out.println(i + " of List 1");
                    try {

                       sendGET("http://localhost:8001/query?q=" + URLEncoder.encode(list.get(i), "UTF-8"));
                    }catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        });

        Thread t2 = new Thread(new Runnable() {
            @Override
            public void run() {
                for(int i = 0; i < list2.size(); i++){
                    System.out.println(i + " of List 2");
                    try {
                       sendGET("http://localhost:8001/query?q=" + URLEncoder.encode(list2.get(i), "UTF-8"));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        });
        Thread t3 = new Thread(new Runnable() {
            @Override
            public void run() {
                String query = "SELECT * FROM YCStudent;";
                try {
                    sendGET("http://localhost:8001/query?q=" + URLEncoder.encode(query, "UTF-8"));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });

        t1.start();
        //t2.start();
        //t3.start();
        try {
           t1.join();
           // t2.join();
            //t3.join();
        } catch (Exception e){
            e.printStackTrace();
        }

    }

    //https://www.programcreek.com/java-api-examples/?class=java.net.HttpURLConnection&method=setRequestProperty
    private static void sendGET(String URL) throws IOException {
        URL obj = new URL(URL);
        HttpURLConnection con = (HttpURLConnection) obj.openConnection(); //open the tunnel
        con.setRequestMethod("GET");
        //con.setRequestProperty("Accept-Encoding", "identity");
        int responseCode = con.getResponseCode();
        System.out.println("GET Response Code :: " + responseCode);
        if (responseCode == HttpURLConnection.HTTP_OK) { // success
            BufferedReader in = new BufferedReader(new InputStreamReader(
                    con.getInputStream()));
            String inputLine;
            StringBuffer response = new StringBuffer();

            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }
            in.close();

            // print result

            //System.out.println(response.toString());
        } else {
            System.out.println("GET request failed");
        }

    }

}