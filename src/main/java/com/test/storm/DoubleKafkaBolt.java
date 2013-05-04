package com.test.storm;

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

public class DoubleKafkaBolt extends BaseRichBolt {
    private OutputCollector collector;
    private Producer<Integer, String> producer;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;

        Properties props = new Properties();

        props.put("metadata.broker.list", "broker1:9092");
        props.put("zk.connect", "localhost:2181");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "1");

        ProducerConfig config = new ProducerConfig(props);
        this.producer = new Producer<Integer, String>(config);
    }

    @Override
    public void execute(Tuple input) {
        String inputParam = input.getString(0);
        StringBuilder sb = new StringBuilder();
        sb.append(inputParam);
        sb.append(inputParam);

        producer.send(new ProducerData<Integer, String>("node-kafka-storm-output", sb.toString()));

        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("double"));
    }
}
