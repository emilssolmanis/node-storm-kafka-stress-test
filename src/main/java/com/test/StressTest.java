package com.test;

import io.socket.IOAcknowledge;
import io.socket.IOCallback;
import io.socket.SocketIO;
import io.socket.SocketIOException;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.json.JSONException;
import org.json.JSONObject;

public class StressTest {
    private static class RequestLatencyTest implements IOCallback {
        private Date receivedTime;
        private volatile String response;
        
        public Date getReceivedTime() {
            return receivedTime;
        }
        
        public void setReceivedTime(Date receivedTime) {
            this.receivedTime = receivedTime;
        }
        
        public RequestLatencyTest() {
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

    public static void main(String[] args) throws Exception {
        Date started = new Date();
        SocketIO socket = new SocketIO("http://localhost:3000/");
        
        RequestLatencyTest reqLat = new RequestLatencyTest();
        socket.connect(reqLat);

        Map<String, String> reqParams = new HashMap<String, String>();
        reqParams.put("paramValue", "1");
        socket.emit("kafkaRequest", reqParams);

        boolean waiting = true;
        // Java gets stuck for some weird reason if we don't implement a loop body
        while(waiting) {
            waiting = reqLat.getResponse() == null;
        }
        socket.disconnect();

        System.out.printf("Took %d millis\n", reqLat.getReceivedTime().getTime() - started.getTime());
    }
}








