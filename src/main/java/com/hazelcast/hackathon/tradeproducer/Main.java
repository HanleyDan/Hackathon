package com.hazelcast.hackathon.tradeproducer;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.errors.TopicExistsException;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;

public class Main {

    final static String CONFIG_FILE = "src/main/resources/ccloud.config";

    public static void main(String[] args) {
        System.out.println("======Starting up");
        System.out.println(System.getProperty("user.dir"));
        Main main = new Main();
        Properties props = loadConfig(CONFIG_FILE);
        // Add additional properties.
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        //props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG,10);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        //props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaJsonSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        main.run(props);
    }

    private void run(Properties props) {
        createTopic("market-trades", props);
        createTopic("client-trades", props);
        boolean marketBuyOrSell = true; //set state for first trade.
        final boolean clientBuyOrSell = false;
        Random marketRandom = new Random();
        Random clientRandom = new Random();
        marketRandom.setSeed(10L);
        clientRandom.setSeed(10L);
        ScheduledExecutorService marketExecutor = Executors.newScheduledThreadPool(1);
        ScheduledExecutorService clientExecutor = Executors.newScheduledThreadPool(1);

        int marketInitialDelay = 2;
        int clientInitialDelay = 1;
        int marketPeriod = 1;
        int clientPeriod = 1;

        marketExecutor.scheduleAtFixedRate(new TradingTask(marketRandom, marketBuyOrSell, "Market", "market-trades", props), marketInitialDelay, marketPeriod, TimeUnit.SECONDS);
        clientExecutor.scheduleAtFixedRate(new TradingTask(clientRandom, clientBuyOrSell, "Client", "client-trades", props), clientInitialDelay, clientPeriod, TimeUnit.SECONDS);
        //quit();
    }



    public static void createTopic(final String topic,
                          final Properties cloudConfig) {
      System.out.println("====== Create Topic if required");
      final NewTopic newTopic = new NewTopic(topic, Optional.empty(), Optional.empty());
      try (final AdminClient adminClient = AdminClient.create(cloudConfig)) {
          adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
      } catch (final InterruptedException | ExecutionException e) {
          // Ignore if TopicExistsException, which may be valid if topic exists
          if (!(e.getCause() instanceof TopicExistsException)) {
              throw new RuntimeException(e);
          }
      }
  }

    public static Properties loadConfig(final String configFile) {
        final Properties cfg = new Properties();
        try {
            if (!Files.exists(Paths.get(configFile))) {
                throw new IOException(configFile + " not found.");
            }
            try (InputStream inputStream = new FileInputStream(configFile)) {
                cfg.load(inputStream);
            }
        } catch (IOException e) {
            System.out.println(e.getMessage());
            System.exit(-1);
        }
        return cfg;
    }

    private void quit() {
        System.out.println("======Shutting down");
        System.exit(0);
    }
}
