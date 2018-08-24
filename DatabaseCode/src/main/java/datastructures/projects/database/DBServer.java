package datastructures.projects.database;
/*
 * This is the DBServer class sends the query to all my database logic which in turns sends back a ResultSet object.
 * The server sends the response back to the API server.
 * I made this lock safe using the read and write locks.
 */

import java.io.*;
import java.net.HttpURLConnection;
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

        try {
            File file = new File("Database.txt");
            System.out.println(file.delete());

            DBDriver.getSavedDatabase();
            executor = Executors.newFixedThreadPool(20);
            // the above statement makes a queue which accepts max 20 threads
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
                case "GET": //we're dealing with a select query
                    Map<String, Object> parameters = new HashMap<String, Object>();
                    URI requestedUri = he.getRequestURI();
                    String query = requestedUri.getRawQuery();
                    HttpServerParser.parseQuery(query, parameters);
                    query = (String) parameters.get("q");
                    //to make it lock safe, i used the logic in this guy's code:

                    try {
                        resultSet = DBDriver.executeSELECT(query);
                    } catch (JSQLParserException e) {
                        e.printStackTrace();
                    }
                    String response = "";
                    if(resultSet != null) {
                        response = resultSet.printWholeTable2() + "\nResult: " + resultSet.getQueryResult();
                        he.sendResponseHeaders(200, response.length());
                    } else{
                        response = "Invalid query!";
                        he.sendResponseHeaders(400, response.length());
                    }

                    OutputStream os = he.getResponseBody();
                    os.write(response.getBytes());
                    os.close();
                    break;
                case "POST": //we're dealing with queries that actually write to the table
                    Map<String, Object> parameters2 = new HashMap<String, Object>();
                    URI requestedUri2 = he.getRequestURI();
                    String query2 = requestedUri2.getRawQuery();
                    HttpServerParser.parseQuery(query2, parameters2);
                    query2 = (String) parameters2.get("q");

                    try {
                        resultSet = DBDriver.execute(query2);
                    } catch (JSQLParserException e) {
                        e.printStackTrace();
                    }
                    String response2 = "";
                    if(resultSet != null) {
                       response2 = resultSet.printWholeTable2() + "\nResult: " + resultSet.getQueryResult();
                        he.sendResponseHeaders(HttpURLConnection.HTTP_OK, response2.length());
                    } else {
                        he.sendResponseHeaders(HttpURLConnection.HTTP_BAD_REQUEST, response2.length());
                    }

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












