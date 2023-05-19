package com.msb.client.batch.dto;

import org.apache.rocketmq.common.message.Message;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * description :
 *
 * @author kunlunrepo
 * date :  2023-05-19 17:09
 */
public class ListSplitter implements Iterator<List<Message>> {

    private int sizeLimit = 1000_000;

    private final List<Message> messages; // 需要切分的消息

    private int currIndex; // 当前下标

    public ListSplitter(List<Message> messages) {
        this.messages = messages;
    }

    @Override
    public boolean hasNext() {
        return currIndex < messages.size();
    }

    @Override
    public List<Message> next() {
        int nextIndex = currIndex;
        int totalSize = 0;
        for (; nextIndex < messages.size(); nextIndex++) {
            Message message = messages.get(nextIndex);
            // 获取主题和消息体长度
            int tmpSize = message.getTopic().length() + message.getBody().length;
            // 获取属性key和value的长度
            Map<String, String> properties = message.getProperties();
            for (Map.Entry<String, String> entry : properties.entrySet()) {
                tmpSize += entry.getKey().length() + entry.getValue().length();
            }
            // 增加日志的开销20字节
            tmpSize = tmpSize + 20;
            if (tmpSize > sizeLimit) {
                if (nextIndex - currIndex == 0) {
                    nextIndex ++; // 假如下一个子列表没有元素，则添加这个子列表然后退出循环
                }
                break;
            }
            if (tmpSize + totalSize > sizeLimit) {
                break;
            } else {
                totalSize += tmpSize; // 总长度+当前长度
            }
        }

        List<Message> subList = messages.subList(currIndex, nextIndex);
        currIndex = nextIndex;
        return subList;
    }
}
