package tech.nomads.spring.integration.ip.multiplex

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.integration.MessageTimeoutException

class EchoService {
    private val logger: Logger = LoggerFactory.getLogger(this.javaClass)

    fun test(input: String): String {
        if ("FAIL" == input) {
            throw RuntimeException("Failure Demonstration")
        } else if (input.startsWith("TIMEOUT_TEST")) {
            Thread.sleep(3000)
        }
        return "$input:echo"
    }

    fun noResponse(input: String): Any {
        this.logger.info("noResponse called")
        return when (input) {
            "TIMEOUT_TEST_THROW" -> {
                throw MessageTimeoutException("No response received for $input")
            }
            "TIMEOUT_TEST_MSG" -> input
            else -> {
                MessageTimeoutException("No response received for $input")
            }
        }
    }
}
