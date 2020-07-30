package task;

import kafka.producer.ProducerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Arrays;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;

public class App
{
    public static void main( String[] args )
    {
        Scanner in = new Scanner(System.in);
        System.out.print("Введите название топика: ");
        String nameTopic = in.nextLine();

        KafkaConsumer<String, String> consumer = PropertiesConfig.initConsumer();
        Producer<String, String> producer = PropertiesConfig.initProducer();
        PropertiesConfig.createTopic(nameTopic);
        consumer.subscribe(Arrays.asList(nameTopic));

        String mass = "";

        System.out.println("Введите сообщение для отправки ");
        mass = in.nextLine();
        while(!mass.equals("Выход")) {
           producer.send(new ProducerRecord<String, String>(nameTopic, mass));

            producer.flush();

            ConsumerRecords<String, String>  records = consumer.poll(1000);

            for (ConsumerRecord<String, String> record1 : records)
            {
                System.out.println("offset = " +  record1.offset() + ", value = " + record1.value());
                mass=record1.value();
            }
            mass = in.nextLine();
        }

        consumer.unsubscribe();
        producer.close();

        consumer.close();
        System.out.println("Завершение программы");

    }
}
