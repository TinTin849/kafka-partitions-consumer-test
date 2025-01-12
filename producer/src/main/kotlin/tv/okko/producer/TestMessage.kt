package tv.okko.producer

import java.time.Instant

data class TestMessage (
    val numberData: Int,
    val stringData: String,
    val dateData: Instant,
)