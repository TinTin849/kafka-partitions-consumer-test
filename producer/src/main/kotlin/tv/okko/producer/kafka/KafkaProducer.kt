package tv.okko.producer.kafka

import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.internals.RecordHeader
import org.springframework.beans.factory.annotation.Value
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.SendResult
import org.springframework.stereotype.Component
import tv.okko.producer.TestMessage
import tv.okko.producer.configuration.KafkaProperties
import java.util.concurrent.CompletableFuture

@Component
class KafkaProducer(
    @Value("\${spring.application.name}")
    private val applicationName: String,
    private val kafkaTemplate: KafkaTemplate<String, Any>,
    private val kafkaProperties: KafkaProperties,
) {

    private val xClientServiceHeader = listOf(RecordHeader(X_CLIENT_SERVICE_HEADER, applicationName.toByteArray()))

    fun sendTo1Partition(
        command: TestMessage,
    ): CompletableFuture<SendResult<String, Any>> =
        kafkaTemplate.sendWithServiceHeader(
            kafkaProperties.test1partition.topic,
            command.numberData.toString(),
            command,
        )

    fun sendTo3Partition(
        command: TestMessage,
    ): CompletableFuture<SendResult<String, Any>> =
        kafkaTemplate.sendWithServiceHeader(
            kafkaProperties.test3partition.topic,
            command.numberData.toString(),
            command,
        )

    fun sendTo12Partition(
        command: TestMessage,
    ): CompletableFuture<SendResult<String, Any>> =
        kafkaTemplate.sendWithServiceHeader(
            kafkaProperties.test12partition.topic,
            command.numberData.toString(),
            command,
        )

    fun sendTo122Partition(
        command: TestMessage,
    ): CompletableFuture<SendResult<String, Any>> =
        kafkaTemplate.sendWithServiceHeader(
            kafkaProperties.test12partition2.topic,
            command.numberData.toString(),
            command,
        )

    private fun KafkaTemplate<String, Any>.sendWithServiceHeader(
        topic: String,
        key: String,
        value: Any,
    ): CompletableFuture<SendResult<String, Any>> =
        send(ProducerRecord(topic, null, null, key, value, xClientServiceHeader))

    private companion object {
        private const val X_CLIENT_SERVICE_HEADER = "X-Client-Service"
    }
}