package com.msb.client.normal.producer;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.io.UnsupportedEncodingException;

/**
 * description :
 *
 * @author kunlunrepo
 * date :  2023-05-19 11:05
 */
public class SendSyncMsg {
    public static void main(String[] args) throws MQClientException, UnsupportedEncodingException, MQBrokerException, RemotingException, InterruptedException {
        // 实例化消息生产者Producer
        DefaultMQProducer producer = new DefaultMQProducer("group_test"); // 生产组
        // 设置NameServer的地址
        producer.setNamesrvAddr("192.168.10.150:9876");
        // 启动Producer实例
        producer.start();
        // 同步方式发送10条消息
        for (int i = 0; i < 10; i++) {
            // 创建消息，指定Topic，Tag，消息
            Message msg = new Message("TopicTest", "TagA", ("Hello RocketMQ" + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
            // 同步发送方式
            SendResult sendResult = producer.send(msg);
            // 通过sendResult返回消息判断是否成功送达
            System.out.printf("第" +i+ "条消息---发送结果：%s%n", sendResult);
        }
        // 关闭Producer实例
        producer.shutdown();


    }
}
