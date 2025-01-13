package tv.okko.consumer.service

//import kotlinx.coroutines.reactor.awaitSingleOrNull
//import org.springframework.data.redis.core.ReactiveStringRedisTemplate
//import org.springframework.stereotype.Service

//@Service
//class RedisService(
//    redisTemplate: ReactiveStringRedisTemplate,
//) {
//
//    private val redisOps = redisTemplate.opsForValue()
//
//    suspend fun incrementAndCheck(number: Long): Boolean {
//        val currentSize = redisOps.increment("messageCounter")
//            .awaitSingleOrNull()
//
//        if (currentSize != null && currentSize >= number) {
//            redisOps.set("messageCounter", "0")
//            return true
//        } else {
//            return false
//        }
//    }
//}