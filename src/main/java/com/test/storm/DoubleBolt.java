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
    public void execute(Tuple input) {
        String message = input.getString(0);
        String paramValue = null;
        try {
            Map<String, Object> record = mapper.readValue(message, Map.class);
            paramValue = record.get("paramValue").toString();
        } catch (IOException e) {
            paramValue = "error";
        }
        StringBuilder sb = new StringBuilder();
        sb.append(paramValue);
        sb.append(paramValue);
        collector.emit(input, new Values(sb.toString()));
        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("double"));
    }
}
