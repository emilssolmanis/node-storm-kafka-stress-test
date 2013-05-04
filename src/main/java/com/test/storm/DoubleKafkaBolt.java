package com.test.storm;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.javaapi.producer.ProducerData;
import kafka.producer.ProducerConfig;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class DoubleKafkaBolt extends BaseRichBolt {
    private OutputCollector collector;
    private ObjectMapper mapper;
    private Producer<Integer, String> producer;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.mapper = new ObjectMapper();

        Properties props = new Properties();

        props.put("metadata.broker.list", "broker1:9092");
        props.put("zk.connect", "localhost:2181");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "1");

        ProducerConfig config = new ProducerConfig(props);
        this.producer = new Producer<Integer, String>(config);
    }

    @Override    
    @SuppressWarnings("unchecked")
    public void execute(Tuple input) {
        String message = input.getString(0);
        
        Map<String, Object> record = null;
        try {
            record = mapper.readValue(message, Map.class);
        } catch (IOException e) {
            throw new RuntimeException("Error reading JSON from Kafka!");
        }
        String paramValue = record.get("paramValue").toString();
        StringBuilder sb = new StringBuilder();
        sb.append(paramValue);
        sb.append(paramValue);
        record.put("paramValue", sb.toString());

        try {
            producer.send(new ProducerData<Integer, String>("node-kafka-storm-output", mapper.writeValueAsString(record)));
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Error writing JSON to Kafka!");
        }

        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("double"));
    }
}
