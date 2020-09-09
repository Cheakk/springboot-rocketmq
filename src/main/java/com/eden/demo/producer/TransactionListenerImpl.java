package com.eden.demo.producer;

import java.io.UnsupportedEncodingException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

import com.eden.demo.consumer.OrderConsumer;

import lombok.extern.slf4j.Slf4j;
@Slf4j
public class TransactionListenerImpl implements TransactionListener {

	  private AtomicInteger transactionIndex = new AtomicInteger(0);
	  private AtomicInteger count = new AtomicInteger(0);
	  private ConcurrentHashMap<String, Integer> localTrans = new ConcurrentHashMap<>();
	  @Override
	  public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
	      int value = transactionIndex.getAndIncrement();
	      int status = value % 3;
	      localTrans.put(msg.getTransactionId(), status);
	      return LocalTransactionState.UNKNOW;
	  }
	  @Override
	  public LocalTransactionState checkLocalTransaction(MessageExt msg) {
		  String body = null;
		try {
			body = new String(msg.getBody(), "utf-8");
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		  log.info("Consumer-获取消息-主题topic为={}, 消费消息为={}", msg.getTopic(), body);
	      Integer status = localTrans.get(msg.getTransactionId());
	      if (null != status) {
	          switch (status) {
	              case 0:
	                  return LocalTransactionState.UNKNOW;
	              case 1:
	                  return LocalTransactionState.COMMIT_MESSAGE;
	              case 2:
	                  return LocalTransactionState.ROLLBACK_MESSAGE;
	          }
	      }
	      return LocalTransactionState.COMMIT_MESSAGE;
	  }

}
