package my.storm.kafka.demo;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
/**
 * storm-kafka 2015/5/23 0:07
 *
 * @author fuquan
 */
public class MyKafkaTopology {
    public static class KafkaWordSplitter extends BaseRichBolt {

        private static final Logger LOG = LoggerFactory.getLogger(KafkaWordSplitter.class);
        private static final long serialVersionUID = 886149197481637894L;
        private OutputCollector collector;

        @Override
        public void prepare(Map stormConf, TopologyContext context,
                            OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void execute(Tuple input) {
            String line = input.getString(0);
            LOG.info("RECV[kafka -> splitter] " + line);
            String[] words = line.split("\\s+");
            for(String word : words) {
                LOG.info("EMIT[splitter -> counter] " + word);
                collector.emit(input, new Values(word, 1));
            }
            collector.ack(input);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word", "count"));
        }

    }

    public static class WordCounter extends BaseRichBolt {

        private static final Logger LOG = LoggerFactory.getLogger(WordCounter.class);
        private static final long serialVersionUID = 886149197481637894L;
        private OutputCollector collector;
        private Map<String, AtomicInteger> counterMap;

        @Override
        public void prepare(Map stormConf, TopologyContext context,
                            OutputCollector collector) {
            this.collector = collector;
            this.counterMap = new HashMap<String, AtomicInteger>();
        }

        @Override
        public void execute(Tuple input) {
            String word = input.getString(0);
            int count = input.getInteger(1);
            LOG.info("RECV[splitter -> counter] " + word + " : " + count);
            AtomicInteger ai = this.counterMap.get(word);
            if(ai == null) {
                ai = new AtomicInteger();
                this.counterMap.put(word, ai);
            }
            ai.addAndGet(count);
            collector.ack(input);
            LOG.info("CHECK statistics map: " + this.counterMap);
        }

        @Override
        public void cleanup() {
            //TODO : 这个代码 写在这里不行的，storm 提交后是不会执行这个方法。
            LOG.info("The final result:");
            Iterator<Entry<String, AtomicInteger>> iter = this.counterMap.entrySet().iterator();
            File file = new File("/storm/result.txt");
            if(!file.exists()){
                BufferedWriter out = null;
                try {
                    boolean result = file.createNewFile();
                    LOG.error("create file fail or successfully ?  [{}]", result);
                     out = new BufferedWriter(new FileWriter(file));
                    while(iter.hasNext()){
                        Entry<String, AtomicInteger> entry = iter.next();
                        String key = entry.getKey();
                        int value = entry.getValue().get();
                        LOG.error("the result is: [{}] = {}", key, value);
                        out.write(entry.getKey() + "\t:\t" + entry.getValue().get());
                    }
                    out.flush();

                } catch (IOException e) {
                    e.printStackTrace();
                }finally {
                    if(out !=null){
                        try {
                            out.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }



        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word", "count"));
        }
    }

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, InterruptedException {
        String zks = "192.168.184.129:2181";
        String topic = "t1";
        String zkRoot = "/storm"; // default zookeeper root configuration for storm
        String id = "word";

        BrokerHosts brokerHosts = new ZkHosts(zks);
        SpoutConfig spoutConf = new SpoutConfig(brokerHosts, topic, zkRoot, id);
        spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
        spoutConf.forceFromStart = true;
        spoutConf.zkServers = Arrays.asList(new String[] {"192.168.184.129"});
        spoutConf.zkPort = 2181;

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafka-reader", new KafkaSpout(spoutConf), 1); // Kafka我们创建了一个5分区的Topic，这里并行度设置为5
        builder.setBolt("word-splitter", new KafkaWordSplitter(), 1).shuffleGrouping("kafka-reader");
        builder.setBolt("word-counter", new WordCounter()).fieldsGrouping("word-splitter", new Fields("word"));

        Config conf = new Config();

        String name = MyKafkaTopology.class.getSimpleName();
        if (args != null && args.length > 0) {
            // 没有用参数指定，写死了。省得麻烦.
            conf.put(Config.NIMBUS_HOST, "127.0.0.1");
            conf.setNumWorkers(2);
            StormSubmitter.submitTopology("storm-kafka", conf, builder.createTopology());
        } else {
            conf.setMaxTaskParallelism(2);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(name, conf, builder.createTopology());
            Thread.sleep(60000);
            cluster.shutdown();
        }
    }
}
