package com.msb.client.normal.consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

import java.util.List;

/**
 * description :
 *
 * @author kunlunrepo
 * date :  2023-05-19 13:46
 */
public class ClusterConsumer3 {
    public static void main(String[] args) throws MQClientException {
        System.out.println("====================消费者3-开始====================");
        // 实例化消息生产者，指定组名
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("group_consumer");
        // 指定NameSrv地址信息
        consumer.setNamesrvAddr("192.168.10.150:9876");
        // 订阅topic
        consumer.subscribe("TopicTest", "*");
        // 负载均衡模式消息(可以不设置，默认就是负载均衡模式)
        consumer.setMessageModel(MessageModel.CLUSTERING);
        // 注册回调函数，处理消息
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                try {
                    for (MessageExt msg : msgs) {
                        String topic = msg.getTopic();
                        String msgBody = new String(msg.getBody(), "utf-8");
                        String tags = msg.getTags();
                        System.out.println("收到消息："+"topic："+topic+", tags："+tags+", msg"+msgBody);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER; // 稍后再试
                }
                System.out.println("====================消费者3-结束====================");
                return null;
//                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS; // 消费成功
            }
        });
        consumer.start(); // 启动消费者
        System.out.println("Consumer Started.%n");

    }
}
