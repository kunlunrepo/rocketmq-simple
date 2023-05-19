package com.msb.client.tag.consumer;

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
 * date :  2023-05-19 17:45
 */
public class TagConsumer {
    public static void main(String[] args) throws MQClientException {
        // 实例化消息生产者，指定组名
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("TagFilterConsumer");
        // 指定NameSrv地址信息
        consumer.setNamesrvAddr("192.168.10.150:9876");
        // 订阅topic
        consumer.subscribe("TagFilterTest", "TagA || TagB");
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                try {
                    for (MessageExt msg : msgs) {
                        String topic = msg.getTopic();
                        String msgBody = new String(msg.getBody(), "utf-8");
                        String msgPro = msg.getProperty("a");
                        String tags = msg.getTags();
                        System.out.println("收到消息："+"topic: "+ topic +", tags: "+ tags
                                + " ,a: "+ msgPro + " ,msg: "+ msgBody);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.start();
        System.out.println("消费者启动");
    }
}
