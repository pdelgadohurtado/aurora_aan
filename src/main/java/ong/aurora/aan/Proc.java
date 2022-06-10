package ong.aurora.aan;

import com.google.common.hash.Hashing;
import org.apache.kafka.streams.TopologyDescription;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.charset.StandardCharsets;

public class Proc implements Processor<String, String, String, String> {

    private static final Logger log = LoggerFactory.getLogger(Proc.class);

    private ProcessorContext<String, String> context;

    private int registryNumber;

    @Override
    public void init(ProcessorContext<String, String> context) {
        Processor.super.init(context);
        this.context = context;
        // TODO: recuperar el valor actual al iniciar el procesador
        this.registryNumber = 0;
    }

    @Override
    public void process(Record<String, String> record) {

        System.out.println("Procesando un nuevo registro");

        String body = record.key().concat(record.value()); // HASHEAR BODY TODO: hashear en conjunto con el hash anterior

        String sha256hex = Hashing.sha256()
                .hashString(body, StandardCharsets.UTF_8)
                .toString();

        String k = String.valueOf(this.registryNumber); // OBTENER EL ID DE EVENTO ACTUAL

        Record<String, String> newRecord = new Record<>(k, sha256hex, record.timestamp()); // CREAR NUEVO EVENTO

        context.forward(newRecord); // ENVIA EL REGISTRO HACIA EL DOWNSTREAM
        this.registryNumber = this.registryNumber + 1; // AUMENTA EL ID DE EVENTO

    }

    @Override
    public void close() {
        Processor.super.close();
    }


}
