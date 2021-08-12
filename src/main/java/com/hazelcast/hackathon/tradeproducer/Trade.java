package com.hazelcast.hackathon.tradeproducer;

import com.hazelcast.jet.datamodel.Tuple3;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;


public class Trade {
    private static List<String> symbols;
    private static  Map<String, Integer> symbolToPrice;
    private static final int OPENING_PRICE = 2_500;
    private static final int LOWEST_QUANTITY = 10;
    private static final int HIGHEST_QUANTITY = 10_000;


    static {
        Map<String, Tuple3<String, NasdaqMarketCategory, NasdaqFinancialStatus>>
                nasdaqListed = null;
        try {
            nasdaqListed = Utils.nasdaqListed();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }

        symbols = new ArrayList<>(nasdaqListed.keySet());
        //System.out.println("======Symbols"+symbols);
        symbolToPrice = nasdaqListed.entrySet().stream()
                .collect(Collectors.<Map.Entry<String,
                        Tuple3<String, NasdaqMarketCategory, NasdaqFinancialStatus>>,
                        String, Integer>toMap(
                        entry -> entry.getKey(),
                        entry -> OPENING_PRICE));
    }


    String id;
    String side;    // BUY / SELL
    Long timestamp;
    String symbol;
    Long price;
    Long quantity;
    String orderType; // Client / Market

    public Trade(String side, String ordertype, Random random) {
        id = UUID.randomUUID().toString();
        this.side = side;
        timestamp = System.currentTimeMillis();
        symbol = symbols.get(random.nextInt(symbols.size()));
        // Vary price between -1 to +2... randomly
        price = Long.valueOf(symbolToPrice.compute(symbol,
                (k, v) -> v + random.nextInt(3)-1));
        quantity = Long.valueOf(random.nextInt(HIGHEST_QUANTITY-LOWEST_QUANTITY)+LOWEST_QUANTITY);
        this.orderType = ordertype;

    }

    @Override
    public String toString() {
        return "Trade{" +
                "id='" + id + '\'' +
                ", side='" + side + '\'' +
                ", timestamp=" + timestamp +
                ", symbol='" + symbol + '\'' +
                ", price=" + price +
                ", quantity=" + quantity +
                ", OrderType='" + orderType + '\'' +
                '}';
    }
}