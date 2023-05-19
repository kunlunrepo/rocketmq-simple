package com.msb.client.normal.producer;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.io.UnsupportedEncodingException;

/**
 * description :
 *
 * @author kunlunrepo
 * date :  2023-05-19 11:28
 */
public class SendAsyncMsg {
    public static void main(String[] args) throws MQClientException, UnsupportedEncodingException, RemotingException, InterruptedException {
        // 实例化消息生产者Producer
        DefaultMQProducer producer = new DefaultMQProducer("group_test"); // 生产组
        // 设置NameServer的地址
        producer.setNamesrvAddr("192.168.10.150:9876");
        // 启动Producer实例
        producer.start();
        for (int i = 0; i < 10; i++) {
            final int index = i;
            // 创建消息，指定Topic，Tag，消息
            Message msg = new Message("TopicTest", "TagA", "OrderID888", ("Hello RocketMQ" + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
            // SendCallback接收异步返回结果的回调
            int finalI = i;
            producer.send(msg, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    System.out.printf("第" + finalI + "条消息---发送成功：%s%n", sendResult);
                }

                @Override
                public void onException(Throwable throwable) {
                    System.out.printf("第" + finalI + "条消息---发送失败：%s%n", throwable);
                }
            });
        }
        Thread.sleep(10000);
        // 关闭Producer实例
        producer.shutdown();
    }
}
