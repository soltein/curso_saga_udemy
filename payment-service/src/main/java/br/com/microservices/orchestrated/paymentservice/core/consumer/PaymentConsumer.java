package br.com.microservices.orchestrated.paymentservice.core.consumer;

import br.com.microservices.orchestrated.paymentservice.core.service.PaymentService;
import br.com.microservices.orchestrated.paymentservice.core.utils.JsonUtil;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@AllArgsConstructor
public class PaymentConsumer {
    private final PaymentService paymentService;
    private final JsonUtil jsonUtil;

    @KafkaListener(
            groupId = "${spring.kafka.consumer.group-id}",
            topics = "${spring.kafka.topic.payment-success}"
    )
    public void consumePaymentSuccessEvent(String payload) {
        log.info("Receiving event {} from payment-success topic", payload);
        var event = jsonUtil.toEvent(payload);
        log.info(event.toString());
        paymentService.realizePayment(event);
    }

    @KafkaListener(
            groupId = "${spring.kafka.consumer.group-id}",
            topics = "${spring.kafka.topic.payment-fail}"
    )
    public void consumePaymentFailEvent(String payload) {
        log.info("Receiving event {} from payment-fail topic", payload);
        var event = jsonUtil.toEvent(payload);
        log.info(event.toString());
        paymentService.realizeRefound(event);
    }
}
