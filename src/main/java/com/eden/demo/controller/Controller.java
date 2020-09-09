package com.eden.demo.controller;

import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.eden.demo.config.RocketMqConfig;
import com.eden.demo.producer.Producer;
import com.eden.demo.producer.TransactionProducer;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@RestController
public class Controller {

	@Autowired
	private Producer producer;

	@Autowired
	private TransactionProducer transactionProducer;

	private List<String> mesList;

	/**
	 * 初始化消息
	 */
	public Controller() {
		mesList = new ArrayList<>();
		mesList.add("小小");
		mesList.add("爸爸");
		mesList.add("妈妈");
		mesList.add("爷爷");
		mesList.add("奶奶");
	}
	/**
	 * 先跑起来
	 * 
	 * @return
	 * @throws Exception
	 */
	@RequestMapping("/text/rocketmq")
	public Object callback() throws Exception {
		// 总共发送五次消息
		for (String s : mesList) {
			// 创建生产信息
			Message message = new Message(RocketMqConfig.TOPIC, "testtag", ("小小一家人的称谓:" + s).getBytes());
			// 发送
			SendResult sendResult = producer.getProducer().send(message);
			log.info("输出生产者信息={}", sendResult);
		}
		return "成功";
	}

	/**
	 * 测试顺序消费
	 * 
	 * @return
	 * @throws Exception
	 */
	@RequestMapping("/text/orderSumer")
	public Object orderSumer() throws Exception {
		String[] tags = new String[] { "TagA", "TagC", "TagD" };
		// 订单列表
		List<OrderStep> orderList = new Controller().buildOrders();
		Date date = new Date();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String dateStr = sdf.format(date);
		for (int i = 0; i < 10; i++) {
			// 加个时间前缀
			String body = dateStr + " Hello RocketMQ " + orderList.get(i);
			Message msg = new Message("testorderconsumer", tags[i % tags.length], "KEY" + i, body.getBytes());
			SendResult sendResult = producer.getProducer().send(msg, new MessageQueueSelector() {
				@Override
				public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
					Long id = (Long) arg; // 根据订单id选择发送queue
					long index = id % mqs.size();
					return mqs.get((int) index);
				}
			}, orderList.get(i).getOrderId());// 订单id
			System.out.println(String.format("SendResult status:%s, queueId:%d, body:%s", sendResult.getSendStatus(),
					sendResult.getMessageQueue().getQueueId(), body));
		}

		return "成功";
	}

	/**
	 * 订单的步骤
	 */
	private static class OrderStep {
		private long orderId;
		private String desc;

		public long getOrderId() {
			return orderId;
		}

		public void setOrderId(long orderId) {
			this.orderId = orderId;
		}

		public String getDesc() {
			return desc;
		}

		public void setDesc(String desc) {
			this.desc = desc;
		}

		@Override
		public String toString() {
			return "OrderStep{" + "orderId=" + orderId + ", desc='" + desc + '\'' + '}';
		}
	}

	/**
	 * 生成模拟订单数据
	 */
	private List<OrderStep> buildOrders() {
		List<OrderStep> orderList = new ArrayList<OrderStep>();

		OrderStep orderDemo = new OrderStep();
		orderDemo.setOrderId(15103111039L);
		orderDemo.setDesc("创建");
		orderList.add(orderDemo);

		orderDemo = new OrderStep();
		orderDemo.setOrderId(15103111065L);
		orderDemo.setDesc("创建");
		orderList.add(orderDemo);

		orderDemo = new OrderStep();
		orderDemo.setOrderId(15103111039L);
		orderDemo.setDesc("付款");
		orderList.add(orderDemo);

		orderDemo = new OrderStep();
		orderDemo.setOrderId(15103117235L);
		orderDemo.setDesc("创建");
		orderList.add(orderDemo);

		orderDemo = new OrderStep();
		orderDemo.setOrderId(15103111065L);
		orderDemo.setDesc("付款");
		orderList.add(orderDemo);

		orderDemo = new OrderStep();
		orderDemo.setOrderId(15103117235L);
		orderDemo.setDesc("付款");
		orderList.add(orderDemo);

		orderDemo = new OrderStep();
		orderDemo.setOrderId(15103111065L);
		orderDemo.setDesc("完成");
		orderList.add(orderDemo);

		orderDemo = new OrderStep();
		orderDemo.setOrderId(15103111039L);
		orderDemo.setDesc("推送");
		orderList.add(orderDemo);

		orderDemo = new OrderStep();
		orderDemo.setOrderId(15103117235L);
		orderDemo.setDesc("完成");
		orderList.add(orderDemo);

		orderDemo = new OrderStep();
		orderDemo.setOrderId(15103111039L);
		orderDemo.setDesc("完成");
		orderList.add(orderDemo);

		return orderList;
	}

	@RequestMapping("/text/delay")
	public Object delay() throws Exception {
		int totalMessagesToSend = 100;
		for (int i = 0; i < totalMessagesToSend; i++) {
			Message message = new Message("testdelaytopic", ("Hello scheduled message " + i).getBytes());
			// 设置延时等级3,这个消息将在10s之后发送(现在只支持固定的几个时间,详看delayTimeLevel)
			message.setDelayTimeLevel(3);
			// 发送消息
			producer.getProducer().send(message);
		}
		return "成功";
	}

	@RequestMapping("/text/batch")
	public Object batch() throws Exception {
		List<Message> messages = new ArrayList<>();
		messages.add(new Message("batchtopic", "TagA", "OrderID001", "Hello world 0".getBytes()));
		messages.add(new Message("batchtopic", "TagA", "OrderID002", "Hello world 1".getBytes()));
		messages.add(new Message("batchtopic", "TagA", "OrderID003", "Hello world 2".getBytes()));
		producer.getProducer().send(messages);
		return "成功";
	}

	@RequestMapping("/text/trasation")
	public Object trasation() throws Exception {
		String[] tags = new String[] { "TagA", "TagB", "TagC", "TagD", "TagE" };
		for (int i = 0; i < 10; i++) {
			try {
				Message msg = new Message("trasationTopic", tags[i % tags.length], "KEY" + i,
						("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
				SendResult sendResult = transactionProducer.getProducer().sendMessageInTransaction(msg, null);
				System.out.printf("%s%n", sendResult);
				Thread.sleep(10);
			} catch (MQClientException | UnsupportedEncodingException e) {
				e.printStackTrace();
			}
		}
		return "成功";
	}
}