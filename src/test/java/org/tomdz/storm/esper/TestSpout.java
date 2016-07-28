package org.tomdz.storm.esper;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;

import java.util.List;
import java.util.Map;

public class TestSpout extends BaseRichSpout
{
    private static final long serialVersionUID = 1L;

    private final Fields fields;
    private final List<Object>[] data;
    private transient int curIdx;
    private transient SpoutOutputCollector collector;

    public TestSpout(Fields fields, List<Object>... data)
    {
        this.fields = fields;
        this.data = data;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(fields);
    }

    @Override
    public void nextTuple()
    {
        if (curIdx < data.length) {
            collector.emit(data[curIdx++]);
        }
    }

    @Override
    public void open(@SuppressWarnings("rawtypes") Map conf,
                     TopologyContext context,
                     SpoutOutputCollector collector)
    {
        this.collector = collector;
    }

    @Override
    public void close() {}

    @Override
    public void ack(Object msgId) {}

    @Override
    public void fail(Object msgId) {}
}
