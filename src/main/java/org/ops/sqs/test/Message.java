package org.ops.sqs.test;

public class Message {
  public String queueUrl;
  public String body;

  public Message(String queueUrl, String body) {
    this.queueUrl = queueUrl;
    this.body = body;
  }
}