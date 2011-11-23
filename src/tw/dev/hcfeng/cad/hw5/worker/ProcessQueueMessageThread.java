package tw.dev.hcfeng.cad.hw5.worker;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
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
	private static String machineId = "1";

	/**
	 * @param args
	 */
	public ProcessQueueMessageThread(String mId) {
		try {
			machineId = mId;
			PropertiesCredentials pCredentials = new PropertiesCredentials(
					ProcessQueueMessageThread.class
							.getResourceAsStream("AwsCredentials.properties"));
			sqs = new AmazonSQSClient(pCredentials);
			s3 = new AmazonS3Client(pCredentials);

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public void run() {
		while (true) {
			processMessage();
			try {
				// 設定為15秒偵測一次
				Thread.sleep(15000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	private static int getMax(String[] numbers) {
		int maxNumber = Integer.parseInt(numbers[0]);
		int idx = 0;
		for (String nStr : numbers) {
			int n = Integer.parseInt(nStr);
			if (++idx == 1)
				continue;
			if (n > maxNumber) {
				maxNumber = n;
			}
		}
		return maxNumber;
	}

	private static int getMin(String[] numbers) {
		int minNumber = Integer.parseInt(numbers[0]);
		int idx = 0;
		for (String nStr : numbers) {
			int n = Integer.parseInt(nStr);
			if (++idx == 1)
				continue;
			if (n < minNumber) {
				minNumber = n;
			}
		}
		return minNumber;
	}

	private static long getProduct(String[] numbers) {
		long productNumber = Long.parseLong(numbers[0]);
		int idx = 0;
		for (String nStr : numbers) {
			int n = Integer.parseInt(nStr);
			if (++idx == 1)
				continue;
			productNumber *= n;
		}
		return productNumber;
	}

	private static long getSum(String[] numbers) {
		long sumNumber = Long.parseLong(numbers[0]);
		int idx = 0;
		for (String nStr : numbers) {
			int n = Integer.parseInt(nStr);
			if (++idx == 1)
				continue;
			sumNumber += n;
		}
		return sumNumber;
	}

	private static long getSumOfSquares(String[] numbers) {
		long sosNumber = (long) Math.pow(Long.parseLong(numbers[0]), 2);
		int idx = 0;
		for (String nStr : numbers) {
			int n = Integer.parseInt(nStr);
			if (++idx == 1)
				continue;
			sosNumber += Math.pow(n, 2);
		}
		return sosNumber;
	}

	private static void processMessage() {
		ReceiveMessageRequest rmRequest = new ReceiveMessageRequest(INBOX_URL)
				.withMaxNumberOfMessages(1);
		ReceiveMessageResult rmResult = sqs.receiveMessage(rmRequest);

		// 有訊息才做
		if (rmResult.getMessages().size() > 0) {
			for (Message mesg : rmResult.getMessages()) {
				// 更改逾時時間
				ChangeMessageVisibilityRequest cvRequest = new ChangeMessageVisibilityRequest(
						rmRequest.getQueueUrl(), mesg.getReceiptHandle(), 30);
				sqs.changeMessageVisibility(cvRequest);

				String logContent = "";
				logContent += "Source ->Inbox: \r\n";
				logContent += "\tMessage ID: " + mesg.getMessageId() + "\r\n";
				logContent += "\tMessage Body: " + mesg.getBody() + "\r\n\r\n";

				String responseMsg = calculateMessage(mesg.getBody());

				// 存到outbox
				SendMessageResult smResult = sqs
						.sendMessage(new SendMessageRequest(OUTBOX_URL,
								responseMsg));

				logContent += "Result->Outbox: \r\n";
				logContent += "\tMessage ID: " + smResult.getMessageId()
						+ "\r\n";
				logContent += "\tMessage Body: " + responseMsg + "\r\n\r\n";

				// 刪除訊息
				DeleteMessageRequest dmRequest = new DeleteMessageRequest(
						rmRequest.getQueueUrl(), mesg.getReceiptHandle());
				sqs.deleteMessage(dmRequest);

				logContent += "Process: \r\n ";
				logContent += "\tTime: " + new Date() + "\r\n";
				logContent += "\tInstance: Woker-" + machineId + "號機("+getLocalIpAddress()+")\r\n";

				// 記錄log至s3
				logToS3(logContent);
			}
		}
	}

	private static String getLocalIpAddress() {
		InetAddress inetAddress = null;
		String ipAddress = "unknow";
		try {
			inetAddress = InetAddress.getLocalHost();
			ipAddress = inetAddress.getHostAddress();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}		
		return ipAddress;
	}

	private static String calculateMessage(String msgBody) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		String[] num = msgBody.replace(" ", "").split(",");
		String responseMsg = "";
		responseMsg = "[" + sdf.format(new Date()) + "]  " + machineId + "號機("
				+ getLocalIpAddress() + ")處理了: " + msgBody;
		responseMsg += "==> max:" + getMax(num);
		responseMsg += ", min:" + getMin(num);
		responseMsg += ", product:" + getProduct(num);
		responseMsg += ", sum:" + getSum(num);
		responseMsg += ", sum of squares:" + getSumOfSquares(num);
		return responseMsg;
	}

	private static void logToS3(String logContent) {
		try {

			if (s3.doesBucketExist(LOG_BUCKET_NAME) == false) {
				s3.createBucket(LOG_BUCKET_NAME);
			}
			File logFile = createLogFile(logContent);
			PutObjectRequest poRequest = new PutObjectRequest(LOG_BUCKET_NAME,
					logFile.getName(), logFile);
			s3.putObject(poRequest);

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	private static File createLogFile(String logContent) throws IOException {
		File file = File.createTempFile(LOG_FILE_NAME_PREFIX,
				LOG_FILE_NAME_SIFFIX);
		file.deleteOnExit();

		Writer writer = new OutputStreamWriter(new FileOutputStream(file));
		writer.append(logContent);
		writer.close();

		return file;
	}

}
