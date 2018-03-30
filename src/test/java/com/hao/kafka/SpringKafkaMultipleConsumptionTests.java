package com.hao.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;


@RunWith(SpringRunner.class)
@SpringBootTest
public class SpringKafkaMultipleConsumptionTests {

    @Autowired
    private KafkaTemplate<Long, byte[]> kafkaTemplate;
    //    @Autowired
//    private Listener listener;
    private ObjectMapper objectMapper = new ObjectMapper();

    @Test
    public void contextLoads() throws InterruptedException {
        ListenableFuture<SendResult<Long, byte[]>> future;

        for (int i = 0; i < 100; i++) {
//            future = kafkaTemplate.send("SpringKafkaTopic", "Messsage:" + i);
            String msg = "test" + i;

            try {
                byte[] serializer = objectMapper.writeValueAsBytes(msg);
                future = kafkaTemplate.send("retail.r21-registration-result.1", Long.valueOf(i), serializer);
                future.addCallback(new ListenableFutureCallback<SendResult<Long, byte[]>>() {
                    @Override
                    public void onSuccess(SendResult<Long, byte[]> result) {
                        System.out.println("Sent message: " + result);
                    }

                    @Override
                    public void onFailure(Throwable ex) {
                        System.out.println("Failed to send message");
                    }
                });
                Thread.sleep(200);
            } catch (Exception ex) {

            }

//            if (i < 20) {
//                future = kafkaTemplate.send("retail.r21-registration-result.1", 1L, serializer);
//            } else if (i < 40) {
//                future = kafkaTemplate.send("retail.r21-registration-result.1", "Messsage:" + i);
//            } else {
//                future = kafkaTemplate.send("retail.r21-registration-result.1", "Messsage:" + i);
//            }


        }

//        assertThat(this.listener.countDownLatch0.await(60, TimeUnit.SECONDS)).isTrue();
//        assertThat(this.listener.countDownLatch1.await(60, TimeUnit.SECONDS)).isTrue();
//        assertThat(this.listener.countDownLatch2.await(60, TimeUnit.SECONDS)).isTrue();
    }

}
