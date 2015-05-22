package word.count.topology;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;
import org.apache.storm.guava.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * storm-demo 2015/5/12 10:03
 * @author fuquan
 */
public class WordCounterBolt extends BaseRichBolt {
    private static final long serialVersionUID = -1516641431576135318L;

    static Logger logger = LoggerFactory.getLogger(WordCounterBolt.class);

    private OutputCollector collector;

    private final Map<String, AtomicInteger> counterMap = Maps.newHashMap();
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this. collector = outputCollector;
    }

    @Override
    public void execute(Tuple input) {
        String word = input.getString(0);
        int count = input.getIntegerByField("count"); // 通过Field名称取出对应字段的数据
        AtomicInteger ai = counterMap.get(word);
        if(ai == null) {
            ai = new AtomicInteger(0);
            counterMap.put(word, ai);
        }
        ai.addAndGet(count);
        logger.error("WordCounterBolt: word=" + word + ", count=" + ai.get());

        /**
         * Bolts的主要方法是execute, 它以一个tuple作为输入，bolts使用OutputCollector来发射tuple，
         *  bolts必须要为它处理的每一个tuple调用OutputCollector的ack方法，以通知Storm这个tuple被处理完成了，
         *  从而通知这个tuple的发射者spouts。
         *  一般的流程是： bolts处理一个输入tuple,  发射0个或者多个tuple,
         *  然后调用ack通知storm自己已经处理过这个tuple了。storm提供了一个IBasicBolt会自动调用ack。
         */
        collector.ack(input);

    }

    @Override
    public void cleanup() {
        logger.error("Word count results:=========================================");
        for(Map.Entry<String, AtomicInteger> entry : counterMap.entrySet()) {
            logger.error("\t word=" + entry.getKey() + ", count=" + entry.getValue().get());
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }
}
