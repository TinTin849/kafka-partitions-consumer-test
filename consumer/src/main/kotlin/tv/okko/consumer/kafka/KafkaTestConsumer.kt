package tv.okko.consumer.kafka

import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component
import tv.okko.consumer.configuration.KafkaProperties
import tv.okko.consumer.util.logger
import tv.okko.producer.TestMessage
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.TimeSource

@Component
class KafkaTestConsumer(
    val kafkaProperties: KafkaProperties
) {
    var atomicAllMessageCounter: AtomicInteger = AtomicInteger(0)
    var atomicMessageCounter: AtomicInteger = AtomicInteger(0)
    var runsCounter: AtomicInteger = AtomicInteger(0)
    val timeSource = TimeSource.Monotonic
    var startTime: TimeSource.Monotonic.ValueTimeMark? = null
    var endTime: TimeSource.Monotonic.ValueTimeMark? = null
    val durations = mutableListOf<Long>()

    var atomicAllMessageCounter3: AtomicInteger = AtomicInteger(0)
    var atomicMessageCounter3: AtomicInteger = AtomicInteger(0)
    var runsCounter3: AtomicInteger = AtomicInteger(0)
    val timeSource3 = TimeSource.Monotonic
    var startTime3: TimeSource.Monotonic.ValueTimeMark? = null
    var endTime3: TimeSource.Monotonic.ValueTimeMark? = null
    val durations3 = mutableListOf<Long>()

    var atomicAllMessageCounter12: AtomicInteger = AtomicInteger(0)
    var atomicMessageCounter12: AtomicInteger = AtomicInteger(0)
    var runsCounter12: AtomicInteger = AtomicInteger(0)
    val timeSource12 = TimeSource.Monotonic
    var startTime12: TimeSource.Monotonic.ValueTimeMark? = null
    var endTime12: TimeSource.Monotonic.ValueTimeMark? = null
    val durations12 = mutableListOf<Long>()

    var atomicAllMessageCounter122: AtomicInteger = AtomicInteger(0)
    var atomicMessageCounter122: AtomicInteger = AtomicInteger(0)
    var runsCounter122: AtomicInteger = AtomicInteger(0)
    val timeSource122 = TimeSource.Monotonic
    var startTime122: TimeSource.Monotonic.ValueTimeMark? = null
    var endTime122: TimeSource.Monotonic.ValueTimeMark? = null
    val durations122 = mutableListOf<Long>()

    @KafkaListener(
        id = "#{__listener.kafkaProperties.test1partition.listener.id}",
        topics = ["#{__listener.kafkaProperties.test1partition.topic}"],
        concurrency = "#{__listener.kafkaProperties.test1partition.listener.concurrency}",
        clientIdPrefix = "#{__listener.kafkaProperties.test1partition.listener.clientIdPrefix}",
        idIsGroup = false,
    )
    fun handler1PartitionTest(message: TestMessage) {
        val allMessagesConsumed = atomicAllMessageCounter.incrementAndGet()
        val messagesConsumed = atomicMessageCounter.incrementAndGet()

        if (messagesConsumed == 1) {
            startTime = timeSource.markNow()
        } else if (messagesConsumed >= 1000) {
            endTime = timeSource.markNow()

            atomicMessageCounter.set(0)
            val runsCounterValue = runsCounter.incrementAndGet()

            val duration = endTime!! - startTime!!
            durations.add(duration.inWholeMilliseconds)

            log.warn { "Time spend on 1000 messages: $duration" }

            if (runsCounterValue >= 10 && runsCounterValue % 5 == 0) {
                if (messagesConsumed % 100 == 0) {
                    log.warn { "Durations: $durations" }
                }
                durations.removeIf { it > 200 }
                log.warn { "Avg time per 1000 messages in 10 runs is ${durations.average()}" }
            }
        }

        if (allMessagesConsumed % 250 == 0) {
            log.info { "$allMessagesConsumed messages consumed from 1 partition topic" }
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
        val allMessagesConsumed = atomicAllMessageCounter3.incrementAndGet()
        val messagesConsumed = atomicMessageCounter3.incrementAndGet()

        if (messagesConsumed == 1) {
            startTime3 = timeSource3.markNow()
        } else if (messagesConsumed >= 1000) {
            endTime3 = timeSource3.markNow()

            atomicMessageCounter3.set(0)
            val runsCounterValue = runsCounter3.incrementAndGet()

            val duration = endTime3!! - startTime3!!
            durations3.add(duration.inWholeMilliseconds)

            log.warn { "Time spend on 1000 messages: $duration" }

            if (runsCounterValue >= 10 && runsCounterValue % 5 == 0) {
                if (messagesConsumed % 100 == 0) {
                    log.warn { "Durations: $durations3" }
                }
                durations3.removeIf { it > 200 }
                log.warn { "Avg time per 1000 messages in 10 runs is ${durations3.average()}" }
            }
        }

        if (allMessagesConsumed % 250 == 0) {
            log.info { "$allMessagesConsumed messages consumed from 3 partition topic" }
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
        val allMessagesConsumed = atomicAllMessageCounter12.incrementAndGet()
        val messagesConsumed = atomicMessageCounter12.incrementAndGet()

        if (messagesConsumed == 1) {
            startTime12 = timeSource12.markNow()
        } else if (messagesConsumed >= 1000) {
            endTime12 = timeSource12.markNow()

            atomicMessageCounter12.set(0)
            val runsCounterValue = runsCounter12.incrementAndGet()

            val duration = endTime12!! - startTime12!!
            durations12.add(duration.inWholeMilliseconds)

            log.warn { "Time spend on 1000 messages: $duration" }

            if (runsCounterValue >= 10 && runsCounterValue % 5 == 0) {
                if (messagesConsumed % 100 == 0) {
                    log.warn { "Durations: $durations12" }
                }
                durations12.removeIf { it > 600 }
                log.warn { "Avg time per 1000 messages in 10 runs is ${durations12.average()}" }
            }
        }

        if (allMessagesConsumed % 1000 == 0) {
            log.info { "$allMessagesConsumed messages consumed from 12 partition topic" }
        }
    }

    @KafkaListener(
        id = "#{__listener.kafkaProperties.test12partition2.listener.id}",
        topics = ["#{__listener.kafkaProperties.test12partition2.topic}"],
        concurrency = "#{__listener.kafkaProperties.test12partition2.listener.concurrency}",
        clientIdPrefix = "#{__listener.kafkaProperties.test12partition2.listener.clientIdPrefix}",
        idIsGroup = false,
    )
    fun handler12Partition2Test(message: TestMessage) {
        val allMessagesConsumed = atomicAllMessageCounter122.incrementAndGet()
        val messagesConsumed = atomicMessageCounter122.incrementAndGet()

        if (messagesConsumed == 1) {
            startTime122 = timeSource122.markNow()
        } else if (messagesConsumed >= 1000) {
            endTime122 = timeSource122.markNow()

            atomicMessageCounter122.set(0)
            val runsCounterValue = runsCounter122.incrementAndGet()

            val duration = endTime122!! - startTime122!!
            durations122.add(duration.inWholeMilliseconds)

            log.warn { "Time spend on 1000 messages: $duration" }

            if (runsCounterValue >= 10 && runsCounterValue % 5 == 0) {
                if (messagesConsumed % 100 == 0) {
                    log.warn { "Durations: $durations122" }
                }
                durations122.removeIf { it > 600 }
                log.warn { "Avg time per 1000 messages in 10 runs is ${durations122.average()}" }
            }
        }

        if (allMessagesConsumed % 1000 == 0) {
            log.info { "$allMessagesConsumed messages consumed from 12 partition topic by 3 consumers" }
        }
    }

    private companion object {
        private val log by logger()
    }
}