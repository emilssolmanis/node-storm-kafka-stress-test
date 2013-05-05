package com.test;

import io.socket.IOAcknowledge;
import io.socket.IOCallback;
import io.socket.SocketIO;
import io.socket.SocketIOException;

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONException;
import org.json.JSONObject;

/* Uses https://github.com/Gottox/socket.io-java-client
 * 
 * TODO: At it's current state, this is nonworking, because the socket.io Java client
 * is non-suitable for synchronized use. Check the node.js stress-test.
 */
public class StressTest {
    private static class KafkaLatencyRequest implements IOCallback {
        private Date receivedTime;
        private volatile String response;
        
        public Date getReceivedTime() {
            return receivedTime;
        }
        
        public void setReceivedTime(Date receivedTime) {
            this.receivedTime = receivedTime;
        }
        
        public KafkaLatencyRequest() {
            this.response = null;
        }

        public String getResponse() {
            return this.response;
        }
        
        public void setResponse(String response) {
            this.response = response;
        }

        @Override
        public void onMessage(JSONObject json, IOAcknowledge ack) {
            try {
                System.out.println("Server said:" + json.toString(2));
            } catch (JSONException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void onMessage(String data, IOAcknowledge ack) {
            System.out.println("Server said: " + data);
        }

        @Override
        public void onError(SocketIOException socketIOException) {
            System.out.println("an Error occured");
            socketIOException.printStackTrace();
        }

        @Override
        public void onDisconnect() {
            // System.out.println("Connection terminated.");
        }

        @Override
        public void onConnect() {
            // System.out.println("Connection established");
        }

        @Override
        public void on(String event, IOAcknowledge ack, Object... args) {
            if (event.equals("kafkaResponse")) {
                JSONObject responseMsg = (JSONObject) args[0];
                this.receivedTime = new Date();
                try {
                    this.response = responseMsg.getString("paramValue");
                } catch (JSONException e) {
                    this.response = "";
                }
            }
        }
    }

    private static class LatencySample implements Callable<Long> {
        public LatencySample(String msg) {
            this.msg = msg;
        }

        private String msg;
        
        public String getMsg() {
            return msg;
        }
        
        public void setMsg(String msg) {
            this.msg = msg;
        }

        private String getTargetMsg() {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 4; i++) {
                sb.append(msg);
            }
            return sb.toString();
        }

        @Override
        public Long call() {
            Date started = new Date();
            long startMillis = started.getTime();

            SocketIO socket = null;
            try {
                socket = new SocketIO("http://localhost:3000/");
            } catch (MalformedURLException ex) {
                throw new RuntimeException(ex);
            }
        
            KafkaLatencyRequest reqLat = new KafkaLatencyRequest();
            socket.connect(reqLat);

            Map<String, String> reqParams = new HashMap<String, String>();
            reqParams.put("paramValue", this.msg);
            socket.emit("kafkaRequest", reqParams);

            // wait for max 5s
            while (reqLat.getResponse() == null 
                   && (new Date().getTime() - startMillis) < 5000)  {
            }
            socket.disconnect();
                
            long lat = -1;
            if (reqLat.getResponse() != null ) {
                long receivedMillis = reqLat.getReceivedTime().getTime();
                lat = receivedMillis - startMillis;
                if (lat > 5000) {
                    lat =  -1;
                }
            }

            return lat;
        }
    }

    public static void main(String[] args) throws Exception {
        Logger logger = Logger.getLogger("io.socket");
        logger.setLevel(Level.SEVERE);

        Random random = new Random();
        // ExecutorService executorService = Executors.newCachedThreadPool();
        ExecutorService executorService = Executors.newFixedThreadPool(Integer.parseInt(args[0]));
        List<Callable<Long>> calls = new ArrayList<>();
        int numCalls = Integer.parseInt(args[1]);
        for (int i = 0; i < numCalls; i++) {
            calls.add(new LatencySample(Integer.toString(random.nextInt())));
        }
        System.out.printf("Starting...\n");
        List<Future<Long>> futures = executorService.invokeAll(calls);
        executorService.shutdown();

        System.out.printf("Done, calculating stats...\n");
        double sum = 0;
        long max = Long.MIN_VALUE;
        long min = Long.MAX_VALUE;
        long num = 0;
        for (Future<Long> millis : futures) {
            long m = millis.get();
            if (m >= 0) {
                sum += m;
                num++;
                max = Math.max(max, m);
                min = Math.min(min, m);
            }
        }
        System.out.printf("Mean response time %.4f millis\n", sum / num);
        System.out.printf("Max response time %d millis\n", max);
        System.out.printf("Min response time %d millis\n", min);
        System.exit(0);
    }
}
