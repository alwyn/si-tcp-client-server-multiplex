package tech.nomads.spring.integration.ip.multiplex

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class TcpClientServerMultiplexApplication

fun main(args: Array<String>) {
    runApplication<TcpClientServerMultiplexApplication>(*args)
}
