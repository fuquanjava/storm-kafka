package word.count.topology;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.MessageId;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.Map;

/**
 * storm-demo 2015/5/12 9:58
 *
 * @author fuquan
 */
public class WordSplitterBolt extends BaseRichBolt {
    private static final long serialVersionUID = -198715843427325765L;
    private static final Logger logger = LoggerFactory.getLogger(WordSplitterBolt.class);

    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;

    }

    @Override
    public void execute(Tuple input) {
        MessageId messageId = input.getMessageId();
        logger.error("+++++++++messageId++++++++++++"+messageId.toString());
        String record = input.getString(0);
        if(record != null && !record.trim().isEmpty()) {
            collector.emit(input, new Values(record, 1));
//            Utils.sleep(1000);
            collector.ack(input);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

        declarer.declare(new Fields("word", "count"));
    }

}
