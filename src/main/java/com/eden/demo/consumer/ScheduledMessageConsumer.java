package com.eden.demo.consumer;

import java.util.List;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.stereotype.Component;

import com.eden.demo.config.RocketMqConfig;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class ScheduledMessageConsumer {

	/**
	 * 消费者实体对象
	 */
	private DefaultMQPushConsumer consumer;
	/**
	 * 消费者组
	 */
	public static final String CONSUMER_GROUP = "delay_consumer";

	/**
	 * 通过构造函数 实例化对象
	 */
	public ScheduledMessageConsumer() throws MQClientException {

		consumer = new DefaultMQPushConsumer(CONSUMER_GROUP);
		consumer.setNamesrvAddr(RocketMqConfig.NAME_SERVER);
		// 消费模式:一个新的订阅组第一次启动从队列的最后位置开始消费 后续再启动接着上次消费的进度开始消费
		consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
		// 订阅主题和 标签（ * 代表所有标签)下信息
	     // 订阅Topics
	      consumer.subscribe("testdelaytopic", "*");
	      // 注册消息监听者
	      consumer.registerMessageListener(new MessageListenerConcurrently() {
	          @Override
	          public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> messages, ConsumeConcurrentlyContext context) {
	              for (MessageExt message : messages) {
	                  // Print approximate delay time period
	                  System.out.println("Receive message[msgId=" + message.getMsgId() + "] " + (System.currentTimeMillis() - message.getStoreTimestamp()) + "ms later");
	              }
	              return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
	          }
	      });

		consumer.start();
		System.out.println("消费者 启动成功=======");
	}
}
