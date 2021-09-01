package tech.nomads.spring.integration.ip.multiplex

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.extension.ExtendWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.core.task.SimpleAsyncTaskExecutor
import org.springframework.core.task.TaskExecutor
import org.springframework.integration.config.EnableIntegrationManagement
import org.springframework.integration.ip.tcp.connection.AbstractServerConnectionFactory
import org.springframework.integration.ip.util.TestingUtilities
import org.springframework.messaging.MessagingException
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.junit.jupiter.SpringExtension
import tech.nomads.spring.integration.ip.multiplex.config.Config
import java.util.concurrent.BlockingQueue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(SpringExtension::class)
@SpringBootTest(classes = [Config::class], webEnvironment = SpringBootTest.WebEnvironment.NONE)
@DirtiesContext
@EnableIntegrationManagement(defaultLoggingEnabled = "true")
class TcpClientServerMultiplexDemoTest(
    @Autowired private val gw: Config.SimpleGateway,
    @Autowired private val serverFactory: AbstractServerConnectionFactory
) {

    @BeforeAll
    fun setup() {
        TestingUtilities.waitListening(this.serverFactory, 10000L)
    }

    @Test
    fun testHappyDay() {
        val result: String? = gw.send("999Hello world!") // first 3 bytes is correlationid
        assertEquals("999Hello world!:echo", result)
    }

    @Test
    @Throws(Exception::class)
    fun testMultiPlex() {
        val executor: TaskExecutor = SimpleAsyncTaskExecutor()
        val latch = CountDownLatch(100)
        val results: BlockingQueue<Int> = LinkedBlockingQueue()
        for (i in 100..199) {
            results.add(i)
            executor.execute {
                val result: String? = gw.send(i.toString() + "Hello world!") // first 3 bytes is correlationid
                assertEquals(i.toString() + "Hello world!:echo", result)
                results.remove(i)
                latch.countDown()
            }
        }
        assertTrue(latch.await(60, TimeUnit.SECONDS))
        assertEquals(0, results.size)
    }

    @Test
    fun testTimeoutThrow() {
        try {
            gw.send("TIMEOUT_TEST_THROW")
            fail("expected exception")
        } catch (e: MessagingException) {
            assertThat(e.message).contains("No response received for TIMEOUT_TEST")
        }
    }

    @Test
    fun testTimeoutReturn() {
        try {
            gw.send("TIMEOUT_TEST_RETURN")
            fail("expected exception")
        } catch (e: MessagingException) {
            assertThat(e.message).contains("No response received for TIMEOUT_TEST")
        }
    }

    @Test
    fun testTimeoutButReturnValidMessage() {
        val result = gw.send("TIMEOUT_TEST_MSG")
        assertThat(result).isEqualTo("TIMEOUT_TEST_MSG")
    }
}
