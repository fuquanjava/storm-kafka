package word.count.topology;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * storm-demo 2015/5/12 9:44
 *
 * @author fuquan
 */
public class ProduceRecordSpout extends BaseRichSpout {
    private static final long serialVersionUID = -198715843427325765L;
    static Logger logger = LoggerFactory.getLogger(ProduceRecordSpout.class);

    private SpoutOutputCollector collector;

    private Random random;

    private String [] records;


    public ProduceRecordSpout(String [] records ){
        this.records = records;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("record"));

    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
        random = new Random();

    }

    @Override
    public void nextTuple() {
        Utils.sleep(1000);
        for(int i = 0 ; i< records.length; i ++){
            String record = records[i];
            logger.error("ProduceRecordSpout start emit data ==============");
            // 每个消息流在定义的时候会被分配给一个id，因为单向消息流使用的相当普遍，
            // OutputFieldsDeclarer定义了一些方法让你可以定义一个stream而不用指定这个id。在这种情况下这个stream会分配个值为‘default’默认的id
            collector.emit(new Values(record),i);
            logger.error("ProduceRecordSpout emitted: record= [{}]", record);
//            records[i] = null;
        }
    }

    /**
     *  另外两个比较重要的spout方法是ack和fail。storm在检测到一个tuple被整个topology成功处理的时候调用ack，否则调用fail。storm只对可靠的spout调用ack和fail。
     *
     */

    @Override
    public void ack(Object msgId) {
        logger.error("ack------------ @@@@@@@@@@@@@@@@@ msgId = {}" , msgId);
    }

    @Override
    public void fail(Object msgId) {
        logger.error("fail-------------@@@@@@@@@@@@@@@@@ msgId = {}" , msgId);
    }
}
