package com.test.storm;

import storm.kafka.KafkaConfig.StaticHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;

import com.google.common.collect.ImmutableList;

public class TestTopology {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        
        SpoutConfig spoutConfig = new SpoutConfig(
          StaticHosts.fromHostString(ImmutableList.of("localhost:9092"), 1), // list of Kafka brokers
          "node-kafka-storm", // topic to read from
          "/kafkastorm", // the root path in Zookeeper for the spout to store the consumer offsets
          "discovery"); // an id for this consumer for storing the consumer offsets in Zookeeper
        spoutConfig.scheme = new StringScheme();
        KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);

        builder.setSpout("param", kafkaSpout, 4);
        builder.setBolt("doubleFirst", new DoubleBolt(), 2)
                .shuffleGrouping("param");
        builder.setBolt("doubleKafka", new DoubleKafkaBolt(), 2)
                .shuffleGrouping("doubleFirst");
                
        Config conf = new Config();
        conf.setDebug(true);
        
        if(args != null && args.length > 0) {
            conf.setNumWorkers(2);
            StormSubmitter.submitTopology("test-topology", conf, builder.createTopology());
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", conf, builder.createTopology());
        }
    }
}
