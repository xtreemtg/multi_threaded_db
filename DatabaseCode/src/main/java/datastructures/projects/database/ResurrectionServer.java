package datastructures.projects.database;

import com.sun.net.httpserver.HttpServer;
import sun.net.www.protocol.http.HttpURLConnection;

import java.io.IOException;
import java.net.BindException;
import java.net.URL;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ResurrectionServer  {


    private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    public static void main(String[] args) {
        APIServer.launch();
        DBServer.launch();
        System.out.println("SERVERS STARTED");
        scheduler.scheduleAtFixedRate(new PingAPI(), 0, 1 , TimeUnit.SECONDS);
        scheduler.scheduleAtFixedRate(new PingDB(), 0, 1 , TimeUnit.SECONDS);
    }



    // method came from https://stackoverflow.com/questions/3584210/preferred-java-way-to-ping-an-http-url-for-availability
    private static boolean pingURL(String url) {
        try {
            HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
            connection.setInstanceFollowRedirects( false );
            connection.setRequestMethod("HEAD");
            int responseCode = connection.getResponseCode();
            return !(200 <= responseCode && responseCode <= 399);
        } catch (IOException exception) {
            return true;
        }
    }

    public static class PingAPI implements Runnable{

        private final String url = "http://localhost:8001/ping";;

        @Override
        public void run() {
            //System.out.println("checking api");
            boolean isDown = pingURL(this.url);
            if(isDown){
                try {
                    System.out.println("API Server was down. Restarting..");
                    APIServer.launch();
                }catch (Exception e){
                    e.printStackTrace();
                }
            } //else System.out.println("API Server connected");
        }
    }

    public static class PingDB implements Runnable{

        private final String url = "http://localhost:8000/ping";

        @Override
        public void run() {
            //System.out.println("checking db");
            boolean isDown = pingURL(this.url);
            if(isDown){
                try {
                    System.out.println("DB Server was down. Restarting..");
                    DBServer.launch();
                } catch (Exception e){
                    e.printStackTrace();
                }
            } //else System.out.println("DB Server connected");
        }
    }


}
