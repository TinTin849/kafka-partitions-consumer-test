package tv.okko.consumer.kafka

import org.springframework.beans.factory.annotation.Value
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component
import tv.okko.consumer.configuration.KafkaProperties
import tv.okko.consumer.util.logger
import tv.okko.producer.TestMessage
import kotlin.time.Duration
import kotlin.time.TimeSource

@Component
class KafkaTestConsumer(
    val kafkaProperties: KafkaProperties
) {
    @Volatile
    var allMessageCounter: Int = 0
    @Volatile
    var messageCounter: Int = 0
    var runsCounter: Int = 0
    val timeSource = TimeSource.Monotonic
    var startTime: TimeSource.Monotonic.ValueTimeMark? = null
    var endTime: TimeSource.Monotonic.ValueTimeMark? = null
    val durations = mutableListOf<Long>()
    var fullDuration = Duration.ZERO

    @Volatile
    var allMessageCounter3: Int = 0
    @Volatile
    var messageCounter3: Int = 0
    var runsCounter3: Int = 0
    val timeSource3 = TimeSource.Monotonic
    var startTime3: TimeSource.Monotonic.ValueTimeMark? = null
    var endTime3: TimeSource.Monotonic.ValueTimeMark? = null
    val durations3 = mutableListOf<Long>()
    var fullDuration3 = Duration.ZERO

    @Volatile
    var allMessageCounter12: Int = 0
    @Volatile
    var messageCounter12: Int = 0
    var runsCounter12: Int = 0
    val timeSource12 = TimeSource.Monotonic
    var startTime12: TimeSource.Monotonic.ValueTimeMark? = null
    var endTime12: TimeSource.Monotonic.ValueTimeMark? = null
    val durations12 = mutableListOf<Long>()
    var fullDuration12 = Duration.ZERO

    @KafkaListener(
        id = "#{__listener.kafkaProperties.test1partition.listener.id}",
        topics = ["#{__listener.kafkaProperties.test1partition.topic}"],
        concurrency = "#{__listener.kafkaProperties.test1partition.listener.concurrency}",
        clientIdPrefix = "#{__listener.kafkaProperties.test1partition.listener.clientIdPrefix}",
        idIsGroup = false,
    )
    fun handler1PartitionTest(message: TestMessage) {
        allMessageCounter++
        messageCounter++

        if (messageCounter == 1) {
            startTime = timeSource.markNow()
        } else if (messageCounter >= 1000) {
            endTime = timeSource.markNow()

            messageCounter = 0
            runsCounter++

            val duration = endTime!! - startTime!!
            durations.add(duration.inWholeMilliseconds)

            log.warn { "Time spend on 1000 messages: $duration" }

            if (runsCounter >= 10) {
                log.warn { "Avg time per 1000 messages in 10 runs is ${durations.average()}" }
            }
        }

        if (allMessageCounter % 250 == 0) {
            log.info { "$allMessageCounter messages consumed from 1 partition topic" }
        }
    }

    @KafkaListener(
        id = "#{__listener.kafkaProperties.test3partition.listener.id}",
        topics = ["#{__listener.kafkaProperties.test3partition.topic}"],
        concurrency = "#{__listener.kafkaProperties.test3partition.listener.concurrency}",
        clientIdPrefix = "#{__listener.kafkaProperties.test3partition.listener.clientIdPrefix}",
        idIsGroup = false,
    )
    fun handler3PartitionTest(message: TestMessage) {
        allMessageCounter3++
        messageCounter3++

        if (messageCounter3 == 1) {
            startTime3 = timeSource3.markNow()
        } else if (messageCounter3 >= 1000) {
            endTime3 = timeSource3.markNow()

            messageCounter3 = 0
            runsCounter3++

            val duration = endTime3!! - startTime3!!
            durations3.add(duration.inWholeMilliseconds)

            log.warn { "Time spend on 1000 messages: $duration" }

            if (runsCounter3 >= 10) {
                log.warn { "Avg time per 1000 messages in 10 runs is ${durations3.average()}" }
            }
        }

        if (allMessageCounter3 % 250 == 0) {
            log.info { "$allMessageCounter3 messages consumed from 3 partition topic" }
        }
    }

    @KafkaListener(
        id = "#{__listener.kafkaProperties.test12partition.listener.id}",
        topics = ["#{__listener.kafkaProperties.test12partition.topic}"],
        concurrency = "#{__listener.kafkaProperties.test12partition.listener.concurrency}",
        clientIdPrefix = "#{__listener.kafkaProperties.test12partition.listener.clientIdPrefix}",
        idIsGroup = false,
    )
    fun handler12PartitionTest(message: TestMessage) {
        allMessageCounter12++
        messageCounter12++

        if (messageCounter12 == 1) {
            startTime12 = timeSource12.markNow()
        } else if (messageCounter12 >= 1000) {
            endTime12 = timeSource12.markNow()

            messageCounter12 = 0
            runsCounter12++

            val duration = endTime12!! - startTime12!!
            durations12.add(duration.inWholeMilliseconds)

            log.warn { "Time spend on 1000 messages: $duration" }

            if (runsCounter12 >= 10) {
                log.warn { "Avg time per 1000 messages in 10 runs is ${durations12.average()}" }
            }
        }

        if (allMessageCounter12 % 250 == 0) {
            log.info { "$allMessageCounter12 messages consumed from 12 partition topic" }
        }
    }

    private companion object {
        private val log by logger()
    }
}