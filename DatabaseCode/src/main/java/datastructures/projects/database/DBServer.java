package datastructures.projects.database;
/*
 * This is the DBServer class sends the query to all my database logic which in turns sends back a ResultSet object.
 * The server sends the response back to the API server.
 * I made this lock safe using the read and write locks.
 */

import java.io.*;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.*;
import java.util.concurrent.*;


import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import net.sf.jsqlparser.JSQLParserException;

public class DBServer implements Runnable {
    private static DBServer dbServerInstance;
    private HttpServer httpServer;
    private ExecutorService executor;
    private static final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

    public static void launch() {
        dbServerInstance = new DBServer();
        Thread serverThread = new Thread(dbServerInstance);
        serverThread.start(); //starts a new thread for the server

        try {
            serverThread.join();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        DBServer.launch();
    }

    public void run() {

        //most of this code i got from
        //https://stackoverflow.com/questions/18084038/how-to-get-httpserver-to-create-multiple-httphandlers-in-parallel

        try {
            File file = new File("Database.txt");
            System.out.println(file.delete());

            DBDriver.getSavedDatabase();
            executor = Executors.newFixedThreadPool(20);
            // i believe the above statement makes a queue which accepts max 20 threads
            // and runs each thread FIFO style

            httpServer = HttpServer.create(new InetSocketAddress(8000), 0);
            httpServer.createContext("/query", new MyHandler());
            httpServer.createContext("/ping", new PingHandler());
            httpServer.setExecutor(executor);
            httpServer.start();
            System.out.println("Started DBServer at port " + 8000);

        } catch (Throwable t) {
            t.printStackTrace();
        }
    }


    static class MyHandler implements HttpHandler {

        private static ResultSet resultSet;
        /*
         resultSet is the object returned after all the database logic is executed.
        This object has a list of lists which is the table, as well as
        a list of the column types, and a list of the actual columns
        this ResultSet field gets updated after each query returns a ResultSet
        and it's this object that gets returned back to the APIServer
         */

        public void handle(HttpExchange he) throws IOException {

            String requestMethod = he.getRequestMethod();
            switch (requestMethod) {
                case "GET":
                    Map<String, Object> parameters = new HashMap<String, Object>();
                    URI requestedUri = he.getRequestURI();
                    String query = requestedUri.getRawQuery();
                    HttpServerDemo.P.parseQuery(query, parameters);
                    query = (String) parameters.get("q");
                    //to make it lock safe, i used the logic in this guy's code:
                    //https://examples.javacodegeeks.com/core-java/util/concurrent/locks-concurrent/reentrantlock/java-reentrantreadwritelock-example/
//                    ExecutorService  e = Executors.newSingleThreadExecutor();
//                    List<Callable<Object>> calls = new ArrayList<>();
//                    calls.add(Executors.callable(new Select(query))); //locksafe Select query
//                    try {
//                        e.invokeAll(calls);
//                    } catch (InterruptedException e1) {
//                        e1.printStackTrace();
//                    }
//                    e.shutdown();
                    try {
                        resultSet = DBDriver.executeSELECT(query);
                    } catch (JSQLParserException e) {
                        e.printStackTrace();
                    }
                    String response = "";
                    if(resultSet != null) {
                        response = resultSet.printWholeTable2() + "\nResult: " + resultSet.getQueryResult();
                    }
                    he.sendResponseHeaders(200, response.length());
                    OutputStream os = he.getResponseBody();
                    os.write(response.getBytes());
                    os.close();
                    break;
                case "POST":
                    Map<String, Object> parameters2 = new HashMap<String, Object>();
                    URI requestedUri2 = he.getRequestURI();
                    String query2 = requestedUri2.getRawQuery();
                    HttpServerDemo.P.parseQuery(query2, parameters2);
                    query2 = (String) parameters2.get("q");

                    try {
                        resultSet = DBDriver.execute(query2);
                    } catch (JSQLParserException e) {
                        e.printStackTrace();
                    }
                    String response2 = "";
                    if(resultSet != null) {
                       response2 = resultSet.printWholeTable2() + "\nResult: " + resultSet.getQueryResult();
                    }

                    he.sendResponseHeaders(200, response2.length());
                    OutputStream os2 = he.getResponseBody();
                    os2.write(response2.getBytes());
                    os2.close();
                    break;

            }

        }

    }
    static class PingHandler implements HttpHandler{

        @Override
        public void handle(HttpExchange he) throws IOException {
            String requestMethod = he.getRequestMethod();
            if(requestMethod.equals("HEAD")){
                he.sendResponseHeaders(200, -1);
            }
        }
    }
}

    //https://www.codeproject.com/Tips/1040097/Create-a-Simple-Web-Server-in-Java-HTTP-Server












