package tv.okko.producer.controller

import org.springframework.http.HttpStatus.NO_CONTENT
import org.springframework.web.bind.annotation.*
import tv.okko.producer.TestMessage
import tv.okko.producer.kafka.KafkaProducer
import tv.okko.producer.util.logger
import java.time.Instant
import kotlin.random.Random.Default.nextInt

@RestController
@RequestMapping("v1/kafka-producer")
class MessageController(
    private val kafkaProducer: KafkaProducer,
) {

    @GetMapping
    @ResponseStatus(NO_CONTENT)
    fun sendMessage() {
        val message = generateTestMessage()

        kafkaProducer.sendTo1Partition(message)

        log.info { "Message sent to kafka: $message" }
    }

    @GetMapping("/{amount}/{topic}")
    @ResponseStatus(NO_CONTENT)
    fun sendManyMessages(
        @PathVariable amount: Int,
        @PathVariable topic: Int,
    ) {
        when (topic) {
            1 -> {
                for (number in 1..amount) {
                    if (number % 200 == 0) {
                        Thread.sleep(10)
                    }
                    kafkaProducer.sendTo1Partition(generateTestMessage())
                }
            }
            3 -> {
                for (number in 1..amount) {
                    if (number % 200 == 0) {
                        Thread.sleep(10)
                    }
                    kafkaProducer.sendTo3Partition(generateTestMessage())
                }
            }
            12 -> {
                for (number in 1..amount) {
                    if (number % 200 == 0) {
                        Thread.sleep(10)
                    }
                    kafkaProducer.sendTo12Partition(generateTestMessage())
                }
            }
            else -> {
                log.error { "Incorrect topic: $topic" }
            }
        }

        log.info { "$amount messages sent to kafka topic $topic" }
    }

    private fun generateTestMessage(

    ): TestMessage {
        return TestMessage(
            numberData = nextInt(),
            stringData = nextInt().toString(),
            dateData = Instant.now(),
        )
    }

    private companion object {
        private val log by logger()
    }
}