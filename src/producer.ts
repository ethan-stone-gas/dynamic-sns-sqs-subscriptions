import { SNSClient, PublishCommand } from "@aws-sdk/client-sns";
import { randomBytes } from "crypto";

async function main() {
  const snsClient = new SNSClient({
    region: "us-east-1",
  });

  let i = 0;

  while (true) {
    const severities = ["info", "warning", "error"];

    const severity = severities[Math.floor(Math.random() * severities.length)];

    const command = new PublishCommand({
      TopicArn: "arn:aws:sns:us-east-1:914165346309:messages.fifo",
      Message: JSON.stringify({
        severity,
        message: `message: ${i}, severity: ${severity}`,
        timestamp: new Date().getTime(),
      }),
      MessageGroupId: severity,
      MessageDeduplicationId: randomBytes(16).toString("hex"),
    });

    await snsClient.send(command);

    console.log(`Published message: ${i}, severity: ${severity}`);

    i++;

    await new Promise((resolve) => setTimeout(resolve, 50));
  }
}

main();
