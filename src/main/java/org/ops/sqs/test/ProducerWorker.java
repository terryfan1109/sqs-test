package org.ops.sqs.test;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.SendMessageRequest;

public class ProducerWorker implements Runnable {
  AtomicBoolean exit = new AtomicBoolean(false);
  AmazonSQSClient client;
  BlockingQueue<Message> queue;
  Reportor reportor;

  public ProducerWorker(AmazonSQSClient client, BlockingQueue<Message> queue,
      Reportor reportor) {
    this.client = client;
    this.queue = queue;
    this.reportor = reportor;
  }

  public void stop() {
    exit.set(true);
  }

  @Override
  public void run() {

    try {
      while (!exit.get()) {
        Message msg = queue.poll(1, TimeUnit.SECONDS);
        while (null != msg) {
          try {
            SendMessageRequest request = new SendMessageRequest().withQueueUrl(
                msg.queueUrl).withMessageBody(msg.body);

            client.sendMessage(request);
            reportor.addSuccess();
            msg = queue.poll(1, TimeUnit.SECONDS);
          } catch (AmazonServiceException e) {
            reportor.addFailure();
          } catch (AmazonClientException e) {
            reportor.addFailure();
          }
        }
      }
    } catch (InterruptedException e) {
      // ...
      e.printStackTrace();
    }
  }
}