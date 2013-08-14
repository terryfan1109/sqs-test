package org.ops.sqs.test;

public class Event {
  public String queueUrl;

  public Event(String queueUrl) {
    this.queueUrl = queueUrl;
  }
}
