package word.count.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * storm-demo 2015/5/12 10:18
 *
 * @author fuquan
 */
public class WordCountTopology {
    public static void main(String[] args) throws InterruptedException, AlreadyAliveException, InvalidTopologyException {

        TopologyBuilder builder = new TopologyBuilder();
        String[] records = new String[]{
        "a","b","c"
        };
        // 由1个线程去执行2个并行度，执行过程是先执行完1个，再执行另外1个，不会对结果又有影响
//        builder.setSpout("spout-producer", new ProduceRecordSpout(records), 2).setNumTasks(1);

        // 由2个线程去执行2个并行度，执行过程是2个线程同时执行，结果会重复统计.
//        builder.setSpout("spout-producer", new ProduceRecordSpout(records), 2).setNumTasks(2);

        builder.setSpout("spout-producer", new ProduceRecordSpout(records), 1).setNumTasks(1);

        builder
                .setBolt("bolt-splitter", new WordSplitterBolt(), 1)
                .shuffleGrouping("spout-producer")
                .setNumTasks(1);

        // 统计的并行度是2，不会对结果又影响，因为shuffleGrouping 按照单词分组了.
        builder.setBolt("bolt-counter", new WordCounterBolt(),1)
                .fieldsGrouping("bolt-splitter", new Fields("word"))
                .setNumTasks(1);

        // submit topology
        Config conf = new Config();
        //如果在指定的时间内，一个消息衍生出来的tuple tree未被完全处理成功，则认为此消息未被完整处理,默认30s
        //在bolt里面休眠2秒，此时设置消息树1秒，就会重复发送数据.

//        conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS,1);
//        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 2);
        String name = WordCountTopology.class.getSimpleName();
        if (args != null && args.length > 0) {
            String nimbus = args[0];
            conf.put(Config.NIMBUS_HOST, nimbus);
            conf.setNumWorkers(2);
            StormSubmitter.submitTopologyWithProgressBar(name, conf, builder.createTopology());
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(name, conf, builder.createTopology());
            Thread.sleep(5000);
            cluster.shutdown();
        }


    }
}
