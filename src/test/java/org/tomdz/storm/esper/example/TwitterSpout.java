package org.tomdz.storm.esper.example;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;
import twitter4j.*;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import static org.apache.storm.utils.Utils.tuple;

public class TwitterSpout extends BaseRichSpout implements StatusListener
{
    private static final long serialVersionUID = 1L;

    private final String username;
    private final String pwd;
    private transient BlockingQueue<Status> queue;
    private transient SpoutOutputCollector collector;
    private transient TwitterStream twitterStream;

    public TwitterSpout(String username, String pwd)
    {
        this.username = username;
        this.pwd = pwd;
    }


    @Override
    public void ack(Object arg0)
    {
    }

    @Override
    public void fail(Object arg0)
    {
    }

    @Override
    public void open(@SuppressWarnings("rawtypes") Map conf,
                     TopologyContext context,
                     SpoutOutputCollector collector)
    {
        this.queue = new ArrayBlockingQueue<Status>(1000);
        this.collector = collector;

        Configuration twitterConf = new ConfigurationBuilder().setUser(username).setPassword(pwd).build();
        TwitterStreamFactory fact = new TwitterStreamFactory(twitterConf);

        twitterStream = fact.getInstance();
        twitterStream.addListener(this);
        twitterStream.sample();
    }

    @Override
    public void onException(Exception ex)
    {
    }

    @Override
    public void onStatus(Status status)
    {
        queue.offer(status);
    }

    @Override
    public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice)
    {
    }

    @Override
    public void onTrackLimitationNotice(int numberOfLimitedStatuses)
    {
    }

    @Override
    public void onScrubGeo(long userId, long upToStatusId)
    {
    }

    @Override
    public void close()
    {
        twitterStream.shutdown();
    }

    @Override
    public void nextTuple()
    {
        Status value = queue.poll();
        if (value == null) {
            Utils.sleep(50);
        }
        else {
            collector.emit(tuple(value.getCreatedAt().getTime(), value.getRetweetCount()));            
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields("createdAt", "retweetCount"));
    }
}
