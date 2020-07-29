package task;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Arrays;
import java.util.Scanner;

public class App
{
    public static void main( String[] args )
    {
        Scanner in = new Scanner(System.in);
        System.out.print("Введите название топика: ");
        String nameTopic = in.nextLine();

        PropertiesConfig.createTopic(nameTopic);
        KafkaConsumer<String, String> consumer = PropertiesConfig.initConsumer();
        Producer<String, String> producer = PropertiesConfig.initProducer();
        consumer.subscribe(Arrays.asList(nameTopic));


        String mass = "";

        System.out.println("Введите сообщение для отправки ");
        while(!mass.equals("Выход")) {
            mass = in.nextLine();
            producer.send(new ProducerRecord<String, String>(nameTopic, mass));
            producer.flush();
            ConsumerRecords<String, String> records = consumer.poll(10000);
            for (ConsumerRecord<String, String> record : records)
            {
                System.out.println("offset = " +  record.offset() + ", value = " + record.value());
                mass=record.value();
            }
        }

        consumer.unsubscribe();
        producer.close();

        consumer.close();
        System.out.println("Завершение программы");

    }
}
