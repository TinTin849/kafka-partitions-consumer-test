package tv.okko.consumer.configuration

import org.springframework.boot.context.properties.ConfigurationProperties
import java.time.Duration

@ConfigurationProperties(prefix = "kafka")
data class KafkaProperties(
    val test1partition: TopicListener,
    val test3partition: TopicListener,
    val test12partition: TopicListener,
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