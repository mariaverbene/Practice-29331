package kafkaspring;
import java.io.IOException;
import java.util.*;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.EnableMBeanExport;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.context.IntegrationFlowContext;
import org.springframework.integration.dsl.kafka.Kafka;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.PollableChannel;
import org.springframework.messaging.support.MessageBuilder;


@SpringBootApplication
public class Demo {
    @Autowired
    IntegrationFlowContext flowContext;

    public static void main(String[] args) throws IOException {
        ConfigurableApplicationContext context = new SpringApplicationBuilder(Demo.class).run(args);
        context.getBean(Demo.class).run(context);
    }

    private void run(ConfigurableApplicationContext context) throws IOException {

        PollableChannel consumerChannel = (PollableChannel) context.getBean("consumerChannel");
        MessageChannel producerChannel = (MessageChannel) context.getBean("producerChannel");

        //make listener for consumerChannel topic1
        Map<String, Object> consumerConfigs = (Map<String, Object>) context.getBean("consumerConfigs");
        IntegrationFlow flow = IntegrationFlows
                .from(Kafka.messageDrivenChannelAdapter(
                        new DefaultKafkaConsumerFactory<String, String>(consumerConfigs), "topic1"))
                .channel("consumerChannel")
                .get();
        flowContext.registration(flow).register();
        //

        Message messageReceived;
        Message messageAdj;
        ObjectMapper mapper;
        JsonNode jsonNode;

        while (true) {
            messageReceived = consumerChannel.receive();
            mapper = new ObjectMapper();
            jsonNode = mapper.readTree((String) messageReceived.getPayload());
            ((ObjectNode) jsonNode).put("handledTimestamp", System.currentTimeMillis()); //

            messageAdj = MessageBuilder
                    .withPayload(mapper.writeValueAsString(jsonNode))
                    .setHeader(KafkaHeaders.TOPIC, "topic2")
                    .build();
            producerChannel.send(messageAdj);
        }
    }
}


