package tw.dev.hcfeng.cad.hw5.worker;

public class ProcessQueueMessage {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		String mId = "0";
		if (args.length>0){
			mId = args[0];
		}
		ProcessQueueMessageThread thread = new ProcessQueueMessageThread(mId);
		thread.run();

	}

}
