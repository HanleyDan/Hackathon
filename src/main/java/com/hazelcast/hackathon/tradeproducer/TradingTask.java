package com.hazelcast.hackathon.tradeproducer;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;

public class TradingTask implements Runnable {

    private boolean buyOrSell;
    private Random generator;
    private String orderType;
    private Properties props;
    private Producer<String, Trade> producer;

    public TradingTask(Random generator, boolean buyOrSell, String orderType, String topicName, Properties props) {
        this.buyOrSell = buyOrSell;
        this.generator = generator;
        this.orderType = orderType;
        this.props = props;
        producer = new KafkaProducer<String, Trade>(props);
    }

    public void run() {

        String side = buyOrSell ? "BUY" : "SELL";
        buyOrSell = !buyOrSell;
        Trade trade = new Trade(side, orderType, generator);
        System.out.println("===TRADE: " + trade);

        ProducerRecord<String, Trade> record = new ProducerRecord<>("market-trades", trade.id, trade);
        System.out.println("record built");
        try {
            producer.send(record).get();
            producer.flush();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        System.out.println("record sent");
    };
}