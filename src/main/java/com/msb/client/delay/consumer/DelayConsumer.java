package com.msb.client.delay.consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * description :
 *
 * @author kunlunrepo
 * date :  2023-05-19 16:45
 */
public class DelayConsumer {
    public static void main(String[] args) throws MQClientException {
        // 实例化消息生产者，指定组名
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("ScheduledConsumer");
        // 指定NameSrv地址信息
        consumer.setNamesrvAddr("192.168.10.150:9876");
        // 订阅topic
        consumer.subscribe("ScheduledTopic", "*");
        // 注册消息监听者
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> messages, ConsumeConcurrentlyContext context) {
                for (MessageExt message : messages) {
                    // 打印消息
                    System.out.println("Receive message[msgId="+message.getMsgId()+"]"
                            +(message.getStoreTimestamp() - message.getBornTimestamp()) +"ms later");
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        // 启动消费者
        consumer.start();
    }
}
