package my.storm.kafka.demo;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.*;

import java.util.Arrays;

public class KafkaSpoutTestTopology {
    public static final Logger LOG = LoggerFactory.getLogger(KafkaSpoutTestTopology.class);

    public static class PrinterBolt extends BaseBasicBolt {
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
        }

        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {

            LOG.error("################# [ {} ]#################", tuple.toString());
        }

    }

    private final BrokerHosts brokerHosts;

    public KafkaSpoutTestTopology(String kafkaZookeeper) {
        brokerHosts = new ZkHosts(kafkaZookeeper);
    }

    public StormTopology buildTopology() {
        SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts, "t1", "/storm", "storm-kafka");
        kafkaConfig.zkPort=2181;
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("words", new KafkaSpout(kafkaConfig), 1);
        builder.setBolt("print", new PrinterBolt()).shuffleGrouping("words");
        return builder.createTopology();
    }

    public static void main(String[] args) throws Exception {

        String kafkaZk = "192.168.184.129";
        KafkaSpoutTestTopology kafkaSpoutTestTopology = new KafkaSpoutTestTopology(kafkaZk);
        Config config = new Config();
        config.put(Config.TOPOLOGY_TRIDENT_BATCH_EMIT_INTERVAL_MILLIS, 2000);

        StormTopology stormTopology = kafkaSpoutTestTopology.buildTopology();
        if (args != null && args.length > 1) {
            String dockerIp = "192.168.184.129";
            config.setNumWorkers(1);
            config.setMaxTaskParallelism(2);
            config.put(Config.NIMBUS_HOST, dockerIp);
            config.put(Config.NIMBUS_THRIFT_PORT, 6627);
            config.put(Config.STORM_ZOOKEEPER_PORT, 2181);
            config.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList(dockerIp));
            StormSubmitter.submitTopology("storm-kafka", config, stormTopology);
        } else {
            config.setNumWorkers(1);
            config.setMaxTaskParallelism(1);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("kafka", config, stormTopology);
        }
    }
}
