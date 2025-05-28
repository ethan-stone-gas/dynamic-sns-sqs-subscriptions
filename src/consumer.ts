import {
  SQSClient,
  CreateQueueCommand,
  ReceiveMessageCommand,
  DeleteQueueCommand,
  DeleteMessageCommand,
  SetQueueAttributesCommand,
  ResourceNotFoundException as SQSResourceNotFoundException,
} from "@aws-sdk/client-sqs";
import {
  SNSClient,
  SubscribeCommand,
  UnsubscribeCommand,
  ResourceNotFoundException as SNSResourceNotFoundException,
} from "@aws-sdk/client-sns";
import { randomUUID } from "crypto";

const instanceId = randomUUID();

let subscriptionArn: string | undefined;
let queueUrl: string | undefined;

// State for tracking message latency
const MAX_SAMPLES = 100;
let latencySamples: number[] = [];
let currentAverageLatency = 0;

// Queues for different severity levels
type Severity = "info" | "warning" | "error";
const severityQueues: Map<
  Severity,
  Array<{ message: any; receiptHandle: string }>
> = new Map([
  ["info", []],
  ["warning", []],
  ["error", []],
]);

function updateLatencyAverage(newLatency: number) {
  latencySamples.push(newLatency);
  if (latencySamples.length > MAX_SAMPLES) {
    latencySamples.shift();
  }
  currentAverageLatency =
    latencySamples.reduce((a, b) => a + b, 0) / latencySamples.length;
  console.log(`Current average latency: ${currentAverageLatency.toFixed(2)}ms`);
}

async function processMessage(
  sqsClient: SQSClient,
  severity: Severity,
  message: any,
  receiptHandle: string
) {
  const currentTime = Date.now();
  const messageLatency = currentTime - message.timestamp;
  updateLatencyAverage(messageLatency);

  // Simulate message processing
  await new Promise((resolve) => setTimeout(resolve, 100));

  try {
    await sqsClient.send(
      new DeleteMessageCommand({
        QueueUrl: queueUrl!,
        ReceiptHandle: receiptHandle,
      })
    );
  } catch (error) {
    if (error instanceof Error) {
      if (
        error.name === "InvalidParameterValue" ||
        error.name === "AWS.SimpleQueueService.NonExistentQueue"
      ) {
        console.log(
          `Could not delete message with receipt handle ${receiptHandle}. This is possibly because the queue has been deleted during graceful shutdown.`
        );
      } else {
        throw error;
      }
    } else {
      throw error;
    }
  }
}

async function processSeverityQueue(sqsClient: SQSClient, severity: Severity) {
  while (true) {
    const queue = severityQueues.get(severity);
    if (!queue || queue.length === 0) {
      await new Promise((resolve) => setTimeout(resolve, 100));
      continue;
    }

    const { message, receiptHandle } = queue[0];
    await processMessage(sqsClient, severity, message, receiptHandle);
    queue.shift();
  }
}

async function gracefulShutdown() {
  const snsClient = new SNSClient({
    region: "us-east-1",
  });

  if (!subscriptionArn) {
    console.log("No subscription ARN found");
  } else {
    try {
      const unsubscribe = new UnsubscribeCommand({
        SubscriptionArn: subscriptionArn,
      });

      await snsClient.send(unsubscribe);

      console.log("Unsubscribed from SNS topic");

      subscriptionArn = undefined;
    } catch (error) {
      if (error instanceof SNSResourceNotFoundException) {
        console.log("Subscription not found");
        subscriptionArn = undefined;
      } else {
        throw error;
      }
    }
  }

  const sqsClient = new SQSClient({
    region: "us-east-1",
  });

  if (!queueUrl) {
    console.log("No queue URL found");
  } else {
    try {
      const deleteQueue = new DeleteQueueCommand({
        QueueUrl: queueUrl,
      });

      await sqsClient.send(deleteQueue);

      console.log("Queue deleted");

      queueUrl = undefined;
    } catch (error) {
      console.log(error);
      if (
        error instanceof Error &&
        error.name === "AWS.SimpleQueueService.NonExistentQueue"
      ) {
        console.log("Queue not found");
        queueUrl = undefined;
      } else {
        throw error;
      }
    }
  }
}

const topicArn = "arn:aws:sns:us-east-1:914165346309:messages.fifo";

async function main() {
  process.on("SIGINT", gracefulShutdown);
  process.on("SIGTERM", gracefulShutdown);

  const sqsClient = new SQSClient({
    region: "us-east-1",
  });

  const snsClient = new SNSClient({
    region: "us-east-1",
  });

  // Start processing queues for each severity
  const severityProcessors = ["info", "warning", "error"].map((severity) =>
    processSeverityQueue(sqsClient, severity as Severity)
  );

  console.log("Creating queue");

  const createQueue = new CreateQueueCommand({
    QueueName: `${instanceId}_messages.fifo`,
    Attributes: {
      FifoQueue: "true",
    },
  });

  const createQueueResult = await sqsClient.send(createQueue);

  if (!createQueueResult.QueueUrl) {
    throw new Error("Failed to create queue");
  }

  queueUrl = createQueueResult.QueueUrl;

  console.log("Queue created");

  const queueArn = `arn:aws:sqs:us-east-1:914165346309:${instanceId}_messages.fifo`;

  console.log("Giving permissions to SNS to send messages to queue");

  const setQueueAttributes = new SetQueueAttributesCommand({
    QueueUrl: queueUrl,
    Attributes: {
      Policy: JSON.stringify({
        Version: "2012-10-17",
        Id: `${instanceId}_queue_policy/SNS`,
        Statement: [
          {
            Sid: `${instanceId}_queue_stmt/SNS`,
            Effect: "Allow",
            Principal: {
              Service: "sns.amazonaws.com",
            },
            Action: ["sqs:SendMessage", "sqs:SendMessageBatch"],
            Resource: queueArn,
            Condition: {
              ArnEquals: {
                "aws:SourceArn": topicArn,
              },
            },
          },
        ],
      }),
    },
  });

  await sqsClient.send(setQueueAttributes);

  console.log("Permissions given to SNS to send messages to queue");

  const subscribe = new SubscribeCommand({
    TopicArn: topicArn,
    Protocol: "sqs",
    Endpoint: queueArn,
  });

  const subscribeResult = await snsClient.send(subscribe);

  if (
    !subscribeResult.SubscriptionArn ||
    subscribeResult.SubscriptionArn === "pending confirmation"
  ) {
    throw new Error("Failed to subscribe to SNS topic");
  }

  subscriptionArn = subscribeResult.SubscriptionArn;

  console.log("Subscribed to SNS topic");

  while (true) {
    const receiveMessage = new ReceiveMessageCommand({
      MessageSystemAttributeNames: ["All"],
      QueueUrl: queueUrl,
      MaxNumberOfMessages: 10,
      WaitTimeSeconds: 5,
      VisibilityTimeout: 10,
    });

    try {
      const receiveMessageResult = await sqsClient.send(receiveMessage);

      const messages = receiveMessageResult.Messages;

      if (!messages || messages.length === 0) {
        console.log("No messages received");
        continue;
      }

      console.log(`Received ${messages.length} messages`);

      for (const message of messages) {
        if (!message.Body) {
          continue;
        }

        const body = JSON.parse(message.Body);
        const msg = JSON.parse(body.Message) as {
          severity: Severity;
          message: string;
          timestamp: number;
        };

        // Add message to appropriate severity queue
        const queue = severityQueues.get(msg.severity);
        if (queue) {
          queue.push({ message: msg, receiptHandle: message.ReceiptHandle! });
        }
      }
    } catch (error) {
      if (error instanceof Error) {
        if (error.name === "AWS.SimpleQueueService.NonExistentQueue") {
          console.log("Queue not found");
          break;
        }
      }
      throw error;
    }
  }

  // Wait for all severity processors to complete (they won't in this case)
  await Promise.all(severityProcessors);
}

main().catch(async (error) => {
  console.error(error);
  await gracefulShutdown();
});
