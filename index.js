import pkg from 'aws-sdk';

import dotenv from 'dotenv';
dotenv.config();

const { SQS, SNS, Rekognition } = pkg;

// Create SNS service object.
const sqsClient = new SQS({ region: process.env.REGION });
const snsClient = new SNS({ region: process.env.REGION });
const rekClient = new Rekognition({ region: process.env.REGION });

// Set bucket and video variables
const bucket = process.env.S3_BUCKET; //mntn_s3_bucket_name!
const videoName = 'test-video-creative.mp4'; //change this
const roleArn = process.env.ROLE_ARN;
var startJobId = '';

var ts = Date.now();
const snsTopicName = 'AmazonRekognitionExample' + ts;
const snsTopicParams = { Name: snsTopicName };
const sqsQueueName = 'AmazonRekognitionQueue-' + ts;

// Set the parameters
const sqsParams = {
	QueueName: sqsQueueName, //SQS_QUEUE_URL
	Attributes: {
		DelaySeconds: '60', // Number of seconds delay.
		MessageRetentionPeriod: '86400', // Number of seconds delay.
	},
};

const createTopicandQueue = async () => {
	try {
		// Create SNS topic
		const createTopicPromise = await snsClient.createTopic(snsTopicParams).promise();
		const topicArn = createTopicPromise.TopicArn;

		// Create SQS queue
		const createQueuePromise = await sqsClient.createQueue(sqsParams).promise();
		const sqsQueueUrl = createQueuePromise.QueueUrl;

		const getQueueAttributesPromise = await sqsClient.getQueueAttributes({ QueueUrl: sqsQueueUrl, AttributeNames: ['QueueArn'] }).promise();
		const queueArn = getQueueAttributesPromise.Attributes.QueueArn;

		// Subscribe SQS queue to SNS topic
		const subscribed = await snsClient.subscribe({ TopicArn: topicArn, Protocol: 'sqs', Endpoint: queueArn }).promise();

		const policy = {
			Version: '2012-10-17',
			Statement: [
				{
					Sid: 'MyPolicy',
					Effect: 'Allow',
					Principal: { AWS: '*' },
					Action: 'SQS:SendMessage',
					Resource: queueArn,
					Condition: {
						ArnEquals: {
							'aws:SourceArn': topicArn,
						},
					},
				},
			],
		};

		await sqsClient.setQueueAttributes({ QueueUrl: sqsQueueUrl, Attributes: { Policy: JSON.stringify(policy) } }).promise();

		return [sqsQueueUrl, topicArn];
	} catch (err) {
		console.log(err);
	}
};

const startContentModeration = async (roleArn, snsTopicArn) => {
	const { JobId } = await rekClient
		.startContentModeration({
			Video: {
				S3Object: {
					Bucket: bucket,
					Name: videoName,
				},
			},
			NotificationChannel: {
				SNSTopicArn: snsTopicArn,
				RoleArn: roleArn,
			},
		})
		.promise();

	console.log(`JobID: ${JobId}`);
	return JobId;
};

const getContentModerationResults = async (startJobId) => {
	console.log('Retrieving Label Detection results');
	// Set max results, paginationToken and finished will be updated depending on response values
	var maxResults = 10;
	var paginationToken = '';
	var finished = false;

	// Begin retrieving label detection results
	while (finished == false) {
		let moderationResult = await rekClient
			.getContentModeration({ JobId: startJobId, MaxResults: maxResults, NextToken: paginationToken, SortBy: 'TIMESTAMP' })
			.promise();

		console.log(moderationResult);
	}
};

// Checks for status of job completion
const getSQSMessageSuccess = async (sqsQueueUrl, startJobId) => {
	try {
		// Set job found and success status to false initially
		var jobFound = false;
		var succeeded = false;
		let counter = 0;

		// while not found, continue to poll for response
		while (jobFound == false) {
			const sqsReceivedResponse = await sqsClient
				.receiveMessage({ QueueUrl: sqsQueueUrl, MaxNumberOfMessages: 'ALL', MaxNumberOfMessages: 10 })
				.promise();

			if (sqsReceivedResponse) {
				var responseString = JSON.stringify(sqsReceivedResponse);
				if (!responseString.includes('Body')) {
					console.log('Waiting, be patient!');
					await new Promise((resolve) => setTimeout(resolve, 5000));
					continue;
				}
			}

			// Once job found, log Job ID and return true if status is succeeded
			for (var message of sqsReceivedResponse.Messages) {
				console.log('Retrieved messages:');
				var notification = JSON.parse(message.Body);
				var rekMessage = JSON.parse(notification.Message);
				var messageJobId = rekMessage.JobId;
				if (String(rekMessage.JobId).includes(String(startJobId))) {
					console.log('Matching job found:');
					console.log(rekMessage.JobId);
					jobFound = true;
					console.log(rekMessage.Status);
					if (String(rekMessage.Status).includes(String('SUCCEEDED'))) {
						succeeded = true;
						console.log('Job processing succeeded.');
						const deleteMessage = await sqsClient
							.deleteMessage({ QueueUrl: sqsQueueUrl, ReceiptHandle: message.ReceiptHandle })
							.promise();
					}
				} else {
					console.log('Provided Job ID did not match returned ID.');
					const deleteMessage = await sqsClient.deleteMessage({ QueueUrl: sqsQueueUrl, ReceiptHandle: message.ReceiptHandle }).promise();
				}
			}

			counter++;
		}
		return succeeded;
	} catch (err) {
		console.log('Error', err);
	}
};

// Start label detection job, sent status notification, check for success status
// Retrieve results if status is "SUCEEDED", delete notification queue and topic
const runModerationAndGetResults = async () => {
	try {
		const sqsAndTopic = await createTopicandQueue();
		const startContentModerationRes = await startContentModeration(roleArn, sqsAndTopic[1]);
		const getSQSMessageStatus = await getSQSMessageSuccess(sqsAndTopic[0], startContentModerationRes);
		console.log(getSQSMessageSuccess);
		if (getSQSMessageSuccess) {
			console.log('Retrieving results:');
			const results = await getContentModerationResults(startContentModerationRes);
		}

		//Delete queue and topic
		await sqsClient.deleteQueue({ QueueUrl: sqsAndTopic[0] }).promise();
		await snsClient.deleteTopic({ TopicArn: sqsAndTopic[1] }).promise();

		console.log('Successfully deleted.');
		process.exit();
	} catch (err) {
		console.log('Error', err);
	}
};

runModerationAndGetResults();
