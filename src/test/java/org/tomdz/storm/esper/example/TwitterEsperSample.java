package org.tomdz.storm.esper.example;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;
import org.tomdz.storm.esper.EsperBolt;

public class TwitterEsperSample
{
    public static void main(String[] args)
    {
        final String username = args[0];
        final String pwd = args[1];

        TopologyBuilder builder = new TopologyBuilder();
        TwitterSpout spout = new TwitterSpout(username, pwd);
        EsperBolt bolt = new EsperBolt.Builder()
                                      .inputs().aliasComponent("spout1").toEventType("Tweets")
                                      .outputs().onDefaultStream().emit("tps", "maxRetweets")
                                      .statements().add("select count(*) as tps, max(retweetCount) as maxRetweets from Tweets.win:time_batch(1 sec)")
                                      .build();

        builder.setSpout("spout1", spout);
        builder.setBolt("bolt1", bolt).shuffleGrouping("spout1");

        Config conf = new Config();
        conf.setDebug(true);

        LocalCluster cluster = new LocalCluster();

        cluster.submitTopology("test", conf, builder.createTopology());
        Utils.sleep(10000);
        cluster.shutdown();
    }
}
