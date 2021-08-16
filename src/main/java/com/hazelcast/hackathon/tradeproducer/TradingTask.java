package com.hazelcast.hackathon.tradeproducer;


import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.Random;


public class TradingTask implements Runnable {

    private boolean buyOrSell;
    private Random generator;
    private String orderType;
    private Properties props;
    private String topicName;

    public TradingTask(Random generator, boolean buyOrSell, String orderType, String topicName, Properties props) {
        this.buyOrSell = buyOrSell;
        this.generator = generator;
        this.orderType = orderType;
        this.topicName = topicName;
        this.props = props;

    }

    public void run() {
        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        randomWait();
        String side = buyOrSell ? "BUY" : "SELL";
        buyOrSell = !buyOrSell;
        Trade trade = new Trade(side, orderType, generator);
        System.out.println("===TRADE: " + trade);

        ProducerRecord<String, String> record = new ProducerRecord<String,String>(topicName, trade.id, trade.toString());

       // System.out.println(record);
           producer.send(record, new Callback() {
               @Override
               public void onCompletion(RecordMetadata m, Exception e) {
                   if (e != null) {
                       e.printStackTrace();
                   } else {
                       System.out.printf("Produced record to topic %s partition [%d] @ offset %d%n", m.topic(), m.partition(), m.offset());
                   }
               }
           });


           producer.flush();

        producer.close();
       // System.out.println("record sent");
    };

    private void randomWait() {
        //Wait for a while
        try {
            long wait = (long) (Math.random()*1000);
           // System.out.println(orderType + " " + wait +" millis");
            Thread.sleep((long) (wait));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}