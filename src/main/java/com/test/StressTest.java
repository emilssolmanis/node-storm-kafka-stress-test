package com.test;

import io.socket.IOAcknowledge;
import io.socket.IOCallback;
import io.socket.SocketIO;
import io.socket.SocketIOException;

import java.net.MalformedURLException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import java.util.concurrent.Callable;

import org.json.JSONException;
import org.json.JSONObject;

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
            System.out.println("Connection terminated.");
        }

        @Override
        public void onConnect() {
            System.out.println("Connection established");
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
                
            long receivedMillis = reqLat.getReceivedTime().getTime();
            long lat = receivedMillis - startMillis;
            if (lat > 5000 || !reqLat.getResponse().equals(getTargetMsg())) {
                lat = -1;
            }

            return lat;
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.printf("Took millis\n");
    }
}
