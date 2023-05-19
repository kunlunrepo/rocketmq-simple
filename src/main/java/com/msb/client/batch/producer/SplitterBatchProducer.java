package com.msb.client.batch.producer;

import com.msb.client.batch.dto.ListSplitter;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.ArrayList;
import java.util.List;

/**
 * description :
 *
 * @author kunlunrepo
 * date :  2023-05-19 17:04
 */
public class SplitterBatchProducer {
    public static void main(String[] args) throws MQClientException, MQBrokerException, RemotingException, InterruptedException {
        // 实例化消息生产者Producer
        DefaultMQProducer producer = new DefaultMQProducer("BatchProducer"); // 生产组
        // 设置NameServer的地址
        producer.setNamesrvAddr("192.168.10.150:9876");
        // 启动Producer实例
        producer.start();
        String topic = "BatchTest";
        List<Message> messages = new ArrayList<>(100_000);
        for (int i = 0; i < 100_000; i++) {
           messages.add(new Message(topic, "Tag", "OrderID"+i, ("Hello world " + i).getBytes()));
        }

        // 把大的消息分裂成若干小的消息
        ListSplitter splitter = new ListSplitter(messages);
        while (splitter.hasNext()) {
            List<Message> listItem = splitter.next();
            producer.send(listItem);
            Thread.sleep(100);
        }

        // 关闭Producer实例
        producer.shutdown();

    }
}
