package com.example.taxi;

import com.example.taxi.dto.UserTaxiPositionDTO;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.specific.SpecificData;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.*;
import org.springframework.stereotype.Service;
import taxi.*;

import java.util.*;
import java.util.function.Function;

@Service
public class KafkaStreamTaxi {
    static KafkaStreams streams;
    public static void run() {
        SpecificData.get().addLogicalTypeConversion(new TimeConversions.TimestampConversion());
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "taxi4");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.bootstrap);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        config.put(StreamsConfig.producerPrefix(ProducerConfig.LINGER_MS_CONFIG), 0);
        config.put(StreamsConfig.consumerPrefix(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG), 1);
        config.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG,StreamsConfig.OPTIMIZE);
        config.put("schema.registry.url", Constants.schemaRegistry);
        config.put("specific.avro.reader", "true");

        // When you want to override serdes explicitly/selectively
        final Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url",
                Constants.schemaRegistry);
        // config user position serde
        final Serde<UserPositionKey> userPositionKeySerde = new SpecificAvroSerde<>();
        userPositionKeySerde.configure(serdeConfig, true); // `true` for record keys
        final Serde<UserPositionValue> userPositionValueSerde = new SpecificAvroSerde<>();
        userPositionValueSerde.configure(serdeConfig, false); // `false` for record values

        //config taxi position serde
        final Serde<TaxiPositionKey> taxiPositionKeySerde = new SpecificAvroSerde<>();
        taxiPositionKeySerde.configure(serdeConfig, true); // `true` for record keys
        final Serde<TaxiPositionValue> taxiPositionValueSerde = new SpecificAvroSerde<>();
        taxiPositionValueSerde.configure(serdeConfig, false); // `false` for record values

        //config user table serde
        final Serde<UserTaxiKey> userTaxiKeySerde = new SpecificAvroSerde<>();
        userTaxiKeySerde.configure(serdeConfig, true); // `true` for record keys
        final Serde<UserTaxiValue> userTaxiValueSerde = new SpecificAvroSerde<>();
        userTaxiValueSerde.configure(serdeConfig, false); // `false` for record values

        // config user position serde
        final Serde<UserTaxiPositionKey> userTaxiPositionKeySerde = new SpecificAvroSerde<>();
        userTaxiPositionKeySerde.configure(serdeConfig, true); // `true` for record keys
        final Serde<UserTaxiPositionValue> userTaxiPositionValueSerde = new SpecificAvroSerde<>();
        userTaxiPositionValueSerde.configure(serdeConfig, false); // `false` for record values

        StreamsBuilder builder = new StreamsBuilder();
        // stream userposition
        KTable<String, UserPositionValue> userPositionTable = builder.stream("userposition", Consumed.with(Serdes.String(), userPositionValueSerde)).toTable();
        userPositionTable.toStream().to("userpositiontable");

        // stream taxiposition
        KTable<String, TaxiPositionValue> taxiPositionTable = builder.stream("taxiposition", Consumed.with(Serdes.String(), taxiPositionValueSerde))
                .toTable();

        // stream usertaxi and compact to usertaxitable
        KTable<UserTaxiKey, UserTaxiValue> userTaxiTable = builder.stream("usertaxi", Consumed.with(userTaxiKeySerde, userTaxiValueSerde))
                .toTable().filter((key, value) -> value.getIsActive());

        // get userTaxiTable convert to stream with key user_id
//        KStream<Integer, UserTaxiValue> userTaxiStream = builder.table("usertaxitable", Consumed.with(userTaxiKeySerde, userTaxiValueSerde))
//                .filter((key, value)-> value.getIsActive()).toStream().selectKey((userTaxiKey, userTaxiValue) -> userTaxiKey.getUserId());

        // join all table
        KStream<UserTaxiKey, UserTaxiPositionValue> resultStream = userTaxiTable.leftJoin(
                userPositionTable, new Function<UserTaxiValue, String>() {
                    @Override
                    public String apply(UserTaxiValue userTaxiValue) {
                        return userTaxiValue.getUserId()+"";
                    }
                }, new ValueJoiner<UserTaxiValue, UserPositionValue, UserTaxiPositionValue>() {
                    @Override
                    public UserTaxiPositionValue apply(UserTaxiValue userTaxiValue, UserPositionValue userPositionValue) {
                        if (userPositionValue == null) return null;
                        return UserTaxiPositionValue.newBuilder()
                                .setTaxiId(userTaxiValue.getTaxiId())
                                .setUserLongId(userPositionValue.getLongId())
                                .setUserLatId(userPositionValue.getLatId())
                                .setUserId(userTaxiValue.getUserId())
                                .build();
                    }
                }).leftJoin(taxiPositionTable, new Function<UserTaxiPositionValue, String>() {
                    @Override
                    public String apply(UserTaxiPositionValue userTaxiPositionValue) {
                        if (userTaxiPositionValue == null) return null;
                        return userTaxiPositionValue.getTaxiId()+"";
                    }
                }, new ValueJoiner<UserTaxiPositionValue, TaxiPositionValue, UserTaxiPositionValue>() {
                    @Override
                    public UserTaxiPositionValue apply(UserTaxiPositionValue userTaxiPositionValue, TaxiPositionValue taxiPositionValue) {
                        if (userTaxiPositionValue.getTaxiId() == null) return null;
                        userTaxiPositionValue.put("taxi_long_id", taxiPositionValue.getLongId());
                        userTaxiPositionValue.put("taxi_lat_id", taxiPositionValue.getLatId());
                        return userTaxiPositionValue;
                    }
                }).mapValues(new ValueMapper<UserTaxiPositionValue, UserTaxiPositionValue>() {
                    @Override
                    public UserTaxiPositionValue apply(UserTaxiPositionValue userTaxiPositionValue) {
                        if (userTaxiPositionValue == null) return null;
                        Integer distance = distance(
                                Double.valueOf(userTaxiPositionValue.getUserLatId()),
                                Double.valueOf(userTaxiPositionValue.getTaxiLatId()),
                                Double.valueOf(userTaxiPositionValue.getUserLongId()),
                                Double.valueOf(userTaxiPositionValue.getTaxiLongId()),
                                0.0,0.0);
                        userTaxiPositionValue.put("distance", distance);
                        return userTaxiPositionValue;
                    }
                })
//                .filter( (key, value) -> value.getDistance() < Constants.MAX_DISTANCE)
        .toStream(Named.as("result"));
        resultStream.to("result");

        resultStream.selectKey((key,value) -> UserTaxiPositionKey.newBuilder()
                .setUserId(key.getUserId())
                .setTaxiId(key.getTaxiId())
                .build())
                .toTable(Materialized.<UserTaxiPositionKey, UserTaxiPositionValue, KeyValueStore<Bytes, byte[]>>as("resultquery")
                        .withKeySerde(userTaxiPositionKeySerde)
                        .withValueSerde(userTaxiPositionValueSerde));


        streams = new KafkaStreams(builder.build(), config);
        // only do this in dev - not in prod
        streams.cleanUp();
        streams.start();

        // print the topology
        streams.localThreadsMetadata().forEach(data -> System.out.println(data));

        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }


    public static int distance(double lat1, double lat2, double lon1,
                                  double lon2, double el1, double el2) {

        final int R = 6371; // Radius of the earth

        double latDistance = Math.toRadians(lat2 - lat1);
        double lonDistance = Math.toRadians(lon2 - lon1);
        double a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2)
                + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                * Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        double distance = R * c * 1000; // convert to meters

        double height = el1 - el2;

        distance = Math.pow(distance, 2) + Math.pow(height, 2);

        return (int)Math.round(Math.sqrt(distance));
    }

    public List<UserTaxiPositionDTO> queryByTaxiId(int taxi_id){
        List<UserTaxiPositionDTO> result = new ArrayList<>();

        // When you want to override serdes explicitly/selectively
        final Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url",
                Constants.schemaRegistry);
        // config user position serde
        final Serde<UserTaxiPositionKey> userTaxiPositionKeySerde = new SpecificAvroSerde<>();
        userTaxiPositionKeySerde.configure(serdeConfig, true); // `true` for record keys
        final Serde<UserTaxiPositionValue> userTaxiPositionValueSerde = new SpecificAvroSerde<>();
        userTaxiPositionValueSerde.configure(serdeConfig, false); // `false` for record values


        StreamsBuilder builder = new StreamsBuilder();
        // stream userposition
        ReadOnlyKeyValueStore<UserTaxiPositionKey, UserTaxiPositionValue> store =
                streams.store("resultquery", QueryableStoreTypes.keyValueStore());
        long timeTo = System.currentTimeMillis(); // now (in processing-time)
        long timeFrom = timeTo - 60000 * 5; // beginning of time = oldest available

        KeyValueIterator<UserTaxiPositionKey,UserTaxiPositionValue> iterator = store.all();
        while (iterator.hasNext()) {
            KeyValue<UserTaxiPositionKey, UserTaxiPositionValue> next = iterator.next();
            if(next.key.getTaxiId() == taxi_id){
                UserTaxiPositionDTO userTaxiPositionDTO =
                        new UserTaxiPositionDTO(next.value.getDistance(),
                                next.value.getUserLatId(),
                                next.value.getUserLongId(),
                                next.value.getTaxiLatId(),
                                next.value.getTaxiLongId(),
                                next.value.getTaxiId(),
                                next.value.getUserId());
                result.add(userTaxiPositionDTO);
            }
        }
        return result;

    }

    public List<UserTaxiPositionDTO> queryByUserId(int user_id){
        List<UserTaxiPositionDTO> result = new ArrayList<>();

        // When you want to override serdes explicitly/selectively
        final Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url",
                Constants.schemaRegistry);
        // config user position serde
        final Serde<UserTaxiPositionKey> userTaxiPositionKeySerde = new SpecificAvroSerde<>();
        userTaxiPositionKeySerde.configure(serdeConfig, true); // `true` for record keys
        final Serde<UserTaxiPositionValue> userTaxiPositionValueSerde = new SpecificAvroSerde<>();
        userTaxiPositionValueSerde.configure(serdeConfig, false); // `false` for record values


        StreamsBuilder builder = new StreamsBuilder();
        // stream userposition
        ReadOnlyKeyValueStore<UserTaxiPositionKey, UserTaxiPositionValue> store =
                streams.store("resultquery", QueryableStoreTypes.keyValueStore());
        long timeTo = System.currentTimeMillis(); // now (in processing-time)
        long timeFrom = timeTo - 60000 * 5; // beginning of time = oldest available

        KeyValueIterator<UserTaxiPositionKey,UserTaxiPositionValue> iterator = store.all();
        while (iterator.hasNext()) {
            KeyValue<UserTaxiPositionKey, UserTaxiPositionValue> next = iterator.next();
            if(next.key.getUserId() == user_id){
                UserTaxiPositionDTO userTaxiPositionDTO =
                        new UserTaxiPositionDTO(next.value.getDistance(),
                                next.value.getUserLatId(),
                                next.value.getUserLongId(),
                                next.value.getTaxiLatId(),
                                next.value.getTaxiLongId(),
                                next.value.getTaxiId(),
                                next.value.getUserId());
                result.add(userTaxiPositionDTO);
            }
        }
        return result;

    }
}
