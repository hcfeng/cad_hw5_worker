package tw.dev.hcfeng.cad.hw5.worker;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Date;

import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;

public class ProcessQueueMessageThread extends Thread {

	private static String INBOX_URL = "https://queue.amazonaws.com/792286945666/cad_hw5_inbox";
	private static String OUTBOX_URL = "https://queue.amazonaws.com/792286945666/cad_hw5_outbox";
	private static String LOG_BUCKET_NAME = "cad-hw5";
	private static String LOG_FILE_NAME_PREFIX = "Process_";
	private static String LOG_FILE_NAME_SIFFIX = ".log";
	private static AmazonSQS sqs = null;
	private static AmazonS3 s3 = null;

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			PropertiesCredentials pCredentials = new PropertiesCredentials(
					ProcessQueueMessageThread.class
							.getResourceAsStream("AwsCredentials.properties"));
			sqs = new AmazonSQSClient(pCredentials);
			s3 = new AmazonS3Client(pCredentials);
			processMessage();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void run() {
		while (true) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException exception) {
				// do nothing
			}
		}
	}

	private static int getMax(int[] numbers) {
		int maxNumber = numbers[0];
		int idx = 0;
		for (int n : numbers) {
			if (++idx == 1)
				continue;
			if (n > maxNumber) {
				maxNumber = n;
			}
		}
		return maxNumber;
	}

	private static int getMin(int[] numbers) {
		int minNumber = numbers[0];
		int idx = 0;
		for (int n : numbers) {
			if (++idx == 1)
				continue;
			if (n < minNumber) {
				minNumber = n;
			}
		}
		return minNumber;
	}

	private static long getProduct(int[] numbers) {
		long productNumber = (long) numbers[0];
		int idx = 0;
		for (int n : numbers) {
			if (++idx == 1)
				continue;
			productNumber *= n;
		}
		return productNumber;
	}

	private static long getSum(int[] numbers) {
		long sumNumber = numbers[0];
		int idx = 0;
		for (int n : numbers) {
			if (++idx == 1)
				continue;
			sumNumber += n;
		}
		return sumNumber;
	}

	private static long getSumOfSquares(int[] numbers) {
		long sosNumber = (long) Math.pow(numbers[0], 2);
		int idx = 0;
		for (int n : numbers) {
			if (++idx == 1)
				continue;
			sosNumber += Math.pow(n, 2);
		}
		return sosNumber;
	}

	private static void processMessage() {
		ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(
				INBOX_URL);
		int[] num = { 11, 3, 99, 34, 20, 4, 5, 100 };		
//		List<Message> messages = sqs.receiveMessage(receiveMessageRequest)
//				.getMessages();
//		for (Message message : messages) {			
//			for (Entry<String, String> entry : message.getAttributes()
//					.entrySet()) {
//				System.out.println("  Attribute");
//				System.out.println("    Name:  " + entry.getKey());
//				System.out.println("    Value: " + entry.getValue());
//			}
//		}
		String responseMsg = "";
		responseMsg += "max: " + getMax(num);
		responseMsg += ", min: " + getMin(num);
		responseMsg += ", product: " + getProduct(num);
		responseMsg += ", sum: " + getSum(num);
		responseMsg += ", sum of squares: " + getSumOfSquares(num);
		SendMessageResult rmResult = sqs.sendMessage(new SendMessageRequest(OUTBOX_URL, responseMsg));
//		rmResult.getMessageId();
		logToS3();
	}

	private static void logToS3() {
		try {

			if (s3.doesBucketExist(LOG_BUCKET_NAME) == false) {
				s3.createBucket(LOG_BUCKET_NAME);
			}
			File logFile = createLogFile();
			PutObjectRequest poRequest = new PutObjectRequest(LOG_BUCKET_NAME,
					logFile.getName(), logFile);
			s3.putObject(poRequest);

		} catch (IOException e) {
			// do nothing
		}

	}

	private static File createLogFile() throws IOException {
		File file = File.createTempFile(LOG_FILE_NAME_PREFIX,
				LOG_FILE_NAME_SIFFIX);
		file.deleteOnExit();

		Writer writer = new OutputStreamWriter(new FileOutputStream(file));
		writer.append(new Date() + "");
		writer.close();

		return file;
	}

}
