package org.ops.sqs;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.UnrecognizedOptionException;
import org.ops.sqs.test.Consumer;
import org.ops.sqs.test.Producer;

import com.amazonaws.auth.BasicAWSCredentials;

public class App {

  public static void main(String[] args) {

    Options supportedOptions = new Options()
        .addOption("a", "access_key_id", true, "AWS access key id")
        .addOption("s", "secret_access_key", true, "AWS access secret key")
        .addOption("h", "help", false, "Display help")
        .addOption("q", "queue", true, "Name of queue")
        .addOption("x", "action", true, "Action, enqueue or dequeue")
        .addOption("i", "init_workers", true, "Initial workers")
        .addOption("m", "max_workers", true, "Maximum workers")
        .addOption("n", "number_messages", true, "The Number of messages");

    // parse arguments
    CommandLine cmd = null;
    
    try{
      CommandLineParser parser = new GnuParser();
      cmd = parser.parse(supportedOptions, args);      
      if (!cmd.hasOption("h")) {
        if ("enqueue".equalsIgnoreCase(cmd.getOptionValue("x"))) {
          enqueue_test(cmd);
        }
        else if ("dequeue".equalsIgnoreCase(cmd.getOptionValue("x"))) {
          dequeue_test(cmd);
        }
        else {
          show_usage(supportedOptions);
        }
      } else {
        show_usage(supportedOptions);
      }
    } catch (UnrecognizedOptionException e) {
      System.out.println(e.getMessage());      
      show_usage(supportedOptions);
    } catch (Throwable e) {
      System.out.println(e.getMessage());
    }
  }

  private static void show_usage(Options options) {
    HelpFormatter help = new HelpFormatter();
    help.printHelp("SQS Test Producer", options);    
  }
  
  private static void enqueue_test(CommandLine cmd) {

    String accessKeyId = cmd.getOptionValue("a");
    String accessSecretKey = cmd.getOptionValue("s");
    String queueUrl = cmd.getOptionValue("q");
    int initWorkers = Integer.parseInt(cmd.getOptionValue("i", "1"));
    int maxWorkers = Integer.parseInt(cmd.getOptionValue("m", "5"));
    int numOfMessage = Integer.parseInt(cmd.getOptionValue("n", "10"));
    
    BasicAWSCredentials credentials = new BasicAWSCredentials(accessKeyId,
        accessSecretKey);
    Producer producer = new Producer(numOfMessage, initWorkers, maxWorkers, queueUrl, credentials);
    producer.execute();

  }

  private static void dequeue_test(CommandLine cmd) {

    String accessKeyId = cmd.getOptionValue("a");
    String accessSecretKey = cmd.getOptionValue("s");
    String queueUrl = cmd.getOptionValue("q");
    int initWorkers = Integer.parseInt(cmd.getOptionValue("i", "1"));
    int maxWorkers = Integer.parseInt(cmd.getOptionValue("m", "5"));
    int numOfMessage = Integer.parseInt(cmd.getOptionValue("n", "10"));
    
    BasicAWSCredentials credentials = new BasicAWSCredentials(accessKeyId,
        accessSecretKey);
    Consumer actor = new Consumer(numOfMessage, initWorkers, maxWorkers, queueUrl, credentials);
    actor.execute();

  }

}
