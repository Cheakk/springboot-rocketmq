package com.eden.demo.consumer;

import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.stereotype.Component;

import com.eden.demo.config.RocketMqConfig;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class OrderConsumer {

	/**
	 * 消费者实体对象
	 */
	private DefaultMQPushConsumer consumer;
	/**
	 * 消费者组
	 */
	public static final String CONSUMER_GROUP = "order_consumer";

	/**
	 * 通过构造函数 实例化对象
	 */
	public OrderConsumer() throws MQClientException {

		consumer = new DefaultMQPushConsumer(CONSUMER_GROUP);
		consumer.setNamesrvAddr(RocketMqConfig.NAME_SERVER);
		// 消费模式:一个新的订阅组第一次启动从队列的最后位置开始消费 后续再启动接着上次消费的进度开始消费
		consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
		// 订阅主题和 标签（ * 代表所有标签)下信息
		consumer.subscribe("testorderconsumer", "TagA || TagC || TagD");
		consumer.registerMessageListener(new MessageListenerOrderly() {
			Random random = new Random();
			@Override
			public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
				context.setAutoCommit(true);
				for (MessageExt msg : msgs) {
					// 可以看到每个queue有唯一的consume线程来消费, 订单对每个queue(分区)有序
					System.out.println("consumeThread=" + Thread.currentThread().getName() + "queueId="
							+ msg.getQueueId() + ", content:" + new String(msg.getBody()));
				}
				try {
					// 模拟业务逻辑处理中...
					TimeUnit.SECONDS.sleep(random.nextInt(10));
				} catch (Exception e) {
					e.printStackTrace();
				}
				return ConsumeOrderlyStatus.SUCCESS;
			}
		});

		consumer.start();
		System.out.println("消费者 启动成功=======");
	}
}
