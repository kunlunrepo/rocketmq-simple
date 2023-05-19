package com.msb.client.delay.producer;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 * description :
 *
 * @author kunlunrepo
 * date :  2023-05-19 16:40
 */
public class SendDelayMsg {

    public static void main(String[] args) throws MQClientException, MQBrokerException, RemotingException, InterruptedException {
        // 实例化消息生产者Producer
        DefaultMQProducer producer = new DefaultMQProducer("ScheduledProducer"); // 生产组
        // 设置NameServer的地址
        producer.setNamesrvAddr("192.168.10.150:9876");
        // 启动Producer实例
        producer.start();
        int totalMsgToSend = 10;
        for (int i = 0; i < totalMsgToSend; i++) {
            Message message = new Message("ScheduledTopic", ("Hello scheduled message" + i).getBytes());

            // 设置延时等级
            message.setDelayTimeLevel(4);
            // 发送消息
            producer.send(message);
        }
        // 关闭生产者
        producer.shutdown();
    }
}
