package datastructures.projects.database;
/*
 * This is the API server class, which acts as a proxy between the DBServer and the client
 * The Handler class takes the HTTP query and sends the response to the DBServer (port 8000)
 * it then returns the response that was given by the DBServer back to the client.
 */

import com.sun.deploy.net.URLEncoder;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.*;
import java.net.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class APIServer {

    static HttpServer server = null;
    private static ExecutorService executor;

    public static void launch(){
        InetSocketAddress addr = new InetSocketAddress(8001);
        //HttpServer server = null;
        try {
            server = HttpServer.create(addr, 10);
        } catch (IOException e) {
            e.printStackTrace();
        }

        server.createContext("/query", new APIServer.MyHandler());
        server.createContext("/ping", new PingHandler());

        executor = Executors.newFixedThreadPool(20);
        // the above statement makes a queue which accepts max 20 threads
        // and runs each thread FIFO style
        server.setExecutor(executor);
        server.start();
        System.out.println("APIServer is listening on port 8001");
    }

    static class MyHandler implements HttpHandler{

        public void handle(HttpExchange he) throws IOException {

            String requestMethod = he.getRequestMethod();
            if(requestMethod.equals("GET")) {
                    Map<String, Object> parameters = new HashMap<String, Object>();
                    URI requestedUri = he.getRequestURI();
                    String query = requestedUri.getRawQuery();
                    String s = URLDecoder.decode(query, System.getProperty("file.encoding"));
                    HttpServerParser.parseQuery(s, parameters);

                    //HttpServerParser isn't doing it's job correctly, so I'm hard coding the parsing
                    //for example "UPDATE YCStudent SET Class = 'Super Senior;" would be parsed to
                    // "UPDATE YCStudent SET Class" and that is obviously a problem
                    query = s.substring(2);

                    String response = sendToDBServer(query);
                    //the above method sends the query to the DBServer and receives its response
                    if(response.equals("DB Server is down")){
                        he.sendResponseHeaders(HttpURLConnection.HTTP_INTERNAL_ERROR, response.length());
                    } else if (response.equals("Invalid query!")){
                        he.sendResponseHeaders(HttpURLConnection.HTTP_BAD_REQUEST, response.length());
                    } else he.sendResponseHeaders(HttpURLConnection.HTTP_OK, response.length());

                    OutputStream os = he.getResponseBody();
                    os.write(response.getBytes());
                    os.close();
            }
        }

        private String sendToDBServer(String query) throws IOException{
            String request = "GET";
            if(query == null) {
                query = "";
            }
            else if(query.matches("(SELECT|Delete DB).*")){
                request = "GET";
            } else if(query.matches("(INSERT|CREATE|UPDATE|DELETE).*")){
                request = "POST";
            }
            String URL = "http://localhost:8000/query?q=" + URLEncoder.encode(query, "UTF-8");
            //according to prof. Kelly, the SQL query must be URL Encoded
            java.net.URL obj = new URL(URL);
            HttpURLConnection con = null;
            int responseCode = HttpURLConnection.HTTP_INTERNAL_ERROR;
            boolean connected = false;
            long startTime = System.currentTimeMillis(); //fetch starting time
            //we allow 3.5 seconds to connect with the DBServer and if it doesn't work, we move on and return 500
            while(!connected) {
                long time = System.currentTimeMillis() - startTime;
                if (time > 3500){
                    break;
                }
                try {
                    con = (HttpURLConnection) obj.openConnection(); //open the tunnel
                    con.setRequestMethod(request);
                    responseCode = con.getResponseCode();
                    connected = true;

                } catch (Exception e) {
                   e.printStackTrace();
                }
            }

            if (responseCode == HttpURLConnection.HTTP_OK) { // success
                BufferedReader in = new BufferedReader(new InputStreamReader(
                        con.getInputStream()));
                String inputLine;
                StringBuffer response = new StringBuffer();

                while ((inputLine = in.readLine()) != null) {
                    response.append(inputLine).append("\n");
                }
                in.close();
                return response.toString();

            } else if (responseCode == HttpURLConnection.HTTP_INTERNAL_ERROR) {
                System.out.println("DB Server is down");
                return "DB Server is down";

            }  else if (responseCode == HttpURLConnection.HTTP_BAD_REQUEST) {
                System.out.println("Invalid query!");
                return "Invalid query!";

            }else {
                System.out.println("GET request not worked");
                return "GET request not worked";
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
