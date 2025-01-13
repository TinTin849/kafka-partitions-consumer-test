package tv.okko.producer.configuration

import org.springframework.boot.context.properties.ConfigurationProperties
import java.time.Duration

@ConfigurationProperties(prefix = "kafka")
data class KafkaProperties(
    val test1partition: Topic,
    val test3partition: Topic,
    val test12partition: Topic,
    val test12partition2: Topic,
)

data class Topic(
    val topic: String,
)

data class TopicListener(
    val topic: String,
    val listener: ListenerTopicConfiguration,
)

data class ListenerTopicConfiguration(
    val id: String,
    val concurrency: Int,
    val clientIdPrefix: String,
    val retryTimeout: Duration = Duration.ofSeconds(15),
)