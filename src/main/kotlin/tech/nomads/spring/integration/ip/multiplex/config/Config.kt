package tech.nomads.spring.integration.ip.multiplex.config

import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.event.EventListener
import org.springframework.integration.annotation.IntegrationComponentScan
import org.springframework.integration.annotation.MessagingGateway
import org.springframework.integration.channel.DirectChannel
import org.springframework.integration.channel.PublishSubscribeChannel
import org.springframework.integration.config.EnableIntegration
import org.springframework.integration.dsl.MessageChannels
import org.springframework.integration.dsl.integrationFlow
import org.springframework.integration.ip.dsl.Tcp
import org.springframework.integration.ip.dsl.TcpInboundChannelAdapterSpec
import org.springframework.integration.ip.tcp.connection.AbstractClientConnectionFactory
import org.springframework.integration.ip.tcp.connection.AbstractServerConnectionFactory
import org.springframework.integration.ip.tcp.connection.TcpConnectionEvent
import org.springframework.integration.ip.tcp.serializer.ByteArrayLengthHeaderSerializer
import org.springframework.integration.store.SimpleMessageStore
import org.springframework.messaging.MessageChannel
import tech.nomads.spring.integration.ip.multiplex.EchoService

@EnableIntegration
@IntegrationComponentScan
@Configuration
class Config {
    private val logger = LoggerFactory.getLogger(this.javaClass)

    @MessagingGateway(
        name = "gw",
        defaultRequestChannel = "input",
        defaultReplyTimeout = "20000"
    )
    interface SimpleGateway {
        fun send(request: String): String
    }

    @Bean
    fun echoService() = EchoService()

    @Bean
    fun input() = PublishSubscribeChannel()

    @Bean
    fun noResponseChannel() = DirectChannel()

    @Bean
    fun serializer() = ByteArrayLengthHeaderSerializer(1)

    @Bean
    fun clientFactory(): AbstractClientConnectionFactory =
        Tcp.netClient("127.0.0.1", 10000)
            .lookupHost(false)
            .singleUseConnections(false)
            .soTimeout(10000)
            .serializer(serializer())
            .deserializer(serializer())
            .id("clientFactory")
            .get()

    @Bean
    fun clientOut() =
        integrationFlow {
            publishSubscribe(input(),
                {
                    channel("matchFlow.input")
                },
                {
                    handle(Tcp.outboundAdapter(clientFactory()))
                }
            )
        }

    @Bean
    fun clientIn() =
        integrationFlow(Tcp.inboundAdapter(clientFactory())) {
            transform<ByteArray> { b -> b.toString(Charsets.UTF_8) }
            channel("matchFlow.input")
        }

    @Bean
    fun matchFlow() =
        integrationFlow {
            aggregate {
                headersFunction { g ->
                    g.messages.elementAt(0).headers
                }
                groupTimeout(1000)
                discardChannel(noResponseChannel())
                messageStore(SimpleMessageStore())
                correlationStrategy { m -> (m.payload as String).substring(0,3) }
                releaseStrategy { g -> g.size() == 2 }
                expireGroupsUponCompletion(true)
                expireGroupsUponTimeout(true)
                outputProcessor { g ->
                    g.messages.elementAt(1).payload
                }
            }
        }

    @Bean
    fun noResponseFlow() =
        integrationFlow(noResponseChannel()) {
            handle(echoService(), "noResponse")
        }

    /* ----------------------------------------------------------------------------*/

    @Bean
    fun serverInChannel(): MessageChannel = MessageChannels.direct("serverInChannel").get()

    @Bean
    fun serverFactory(): AbstractServerConnectionFactory =
        Tcp.netServer(10000)
            .serializer(serializer())
            .deserializer(serializer())
            .id("server")
            .get()

    @Bean
    fun serverIn(): TcpInboundChannelAdapterSpec =
        Tcp.inboundAdapter(serverFactory())
            .outputChannel(serverInChannel())
            .id("serverInAdapter")

    @Bean
    fun serverFlow() =
        integrationFlow(serverInChannel()) {
            channel("toTransformer")
            transform<ByteArray> ({ b -> b.toString(Charsets.UTF_8) }) {
                id("serverTransformer")
            }
            channel("toHandler")
            handle(echoService(), "test")
            handle(Tcp.outboundAdapter(serverFactory()))
        }

    @EventListener
    fun logEvent(event: TcpConnectionEvent) = this.logger.info(event.toString())
}
