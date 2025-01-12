import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

buildscript {
    repositories {
        mavenCentral()
        mavenLocal()
    }
}

plugins {
    kotlin("jvm") version "1.9.25"
}

allprojects {
    group = "tv.okko"
}

version = "1.0-SNAPSHOT"

subprojects {
    apply(plugin = "kotlin")


    repositories {
        mavenCentral()
        mavenLocal()
    }

    tasks.withType<Test> {
        useJUnitPlatform()
    }

    tasks.withType<KotlinCompile> {
        kotlinOptions {
            jvmTarget = JavaVersion.VERSION_21.toString()
            freeCompilerArgs = listOf("-Xjsr305=strict")
            allWarningsAsErrors = false
        }
    }
}