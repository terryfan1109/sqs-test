package org.ops.sqs.test;

import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.sqs.AmazonSQSClient;

import org.apache.commons.codec.binary.Base64;

public class Producer {

  private int initWorkers = 1;
  private int maxWorkers = 64;
  private int numOfMessages = 0;
  private String queueUrl;
  private AWSCredentials credentials;

  public Producer(int numOfMessage, int initWorkers, int maxWorkers,
      String queueUrl, AWSCredentials credentials) {

    this.numOfMessages = numOfMessage;
    this.initWorkers = initWorkers;
    this.maxWorkers = maxWorkers;
    this.queueUrl = queueUrl;
    this.credentials = credentials;

  }

  public void execute() {
    Reportor reportor = new Reportor();
    ArrayBlockingQueue<Message> messageQueue = new ArrayBlockingQueue<Message>(
        maxWorkers);

    ProducerWorker[] workers = new ProducerWorker[maxWorkers];
    ArrayBlockingQueue<Runnable> workerQueue = new ArrayBlockingQueue<Runnable>(
        maxWorkers);
    ThreadPoolExecutor threadPool = new ThreadPoolExecutor(initWorkers,
        maxWorkers, 60, TimeUnit.SECONDS, workerQueue);

    for (int i = 0; i < maxWorkers; ++i) {
      AmazonSQSClient client = new AmazonSQSClient(credentials);
      ProducerWorker worker = new ProducerWorker(client, messageQueue, reportor);
      threadPool.execute(worker);
      workers[i] = worker;
    }

    long start = System.currentTimeMillis();
    Random randGen = new Random();
    byte[] randMessage = new byte[2 * 1024];
    for (int i = 0; i < numOfMessages; ++i) {
      randGen.nextBytes(randMessage);
      String msg = Base64.encodeBase64URLSafeString(randMessage);

      try {
        messageQueue.put(new Message(queueUrl, msg));
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    System.out.println("signal workers to stop");

    for (int i = 0; i < workers.length; ++i) {
      workers[i].stop();
    }

    System.out.println("shutdown workers");

    try {
      threadPool.shutdown();
      threadPool.awaitTermination(86400, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      // ...
    }
    long elapse = System.currentTimeMillis() - start;

    System.out.println(String.format(
        "Complete : rps=%f (in %d ms), success=%d, failure=%d",
        (numOfMessages * 1000.0) / elapse, elapse, reportor.getSuccess(),
        reportor.getFailure()));
  }
}
