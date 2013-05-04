package com.test.storm;

import java.io.IOException;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class DoubleBolt extends BaseRichBolt {
    private OutputCollector collector;
    private ObjectMapper mapper;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.mapper = new ObjectMapper();
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
            collector.emit(input, new Values(mapper.writeValueAsString(record)));
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Error emitting JSON!");
        }
        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("double"));
    }
}
