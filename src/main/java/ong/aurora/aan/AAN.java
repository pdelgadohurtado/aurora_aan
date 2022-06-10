package ong.aurora.aan;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ObjectInputFilter;
import java.util.Properties;



class AAN {

    private static final Logger log = LoggerFactory.getLogger(AAN.class);

    public static void main(String[] args) {
        log.info("Iniciando AAN");
        Topology builder = new Topology();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "dev");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
//        props.put(StreamsConfig.APPLICATION_SERVER_CONFIG, endpoint);
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams");

        // TOPIC COMANDOS
        builder.addSource(
                "Aurora AAN Commands", // name
                Serdes.String().deserializer(), // deserializador key
                Serdes.String().deserializer(), // deserializador value
                "aurora-aan-commands"); // topic

        // PROCESADOR
        builder.addProcessor(
                "Aurora AAN Command Validator", // name
                Proc::new, // clase que procesa
                "Aurora AAN Commands"); // parent

        // TOPIC EVENTOS
        builder.addSink(
                "Aurora AAN Events", // name
                "aurora-aan-events", // topic
                Serdes.String().serializer(), // serializador key
                Serdes.String().serializer(), // serializador value
                "Aurora AAN Command Validator"); // parent


        KafkaStreams streams = new KafkaStreams(builder, props);

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        System.out.println("Iniciando streams");
        streams.start();

    }
}