package tv.okko.producer

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.ConfigurationPropertiesScan
import org.springframework.boot.runApplication
import tv.okko.producer.kafka.KafkaProducer

@SpringBootApplication
@ConfigurationPropertiesScan(
    "tv.okko.producer.configuration"
)
class ProducerApplication

fun main(args: Array<String>) {
    runApplication<ProducerApplication>(*args)
}
