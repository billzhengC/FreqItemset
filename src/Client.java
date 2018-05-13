import java.io.BufferedReader;
import java.io.FileReader;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.TreeMap;

public class Client {
	private static ArrayList<String> transactions = new ArrayList<String>();
	public static void main(String[] args) throws Exception {
		TreeMap<String, Integer> summary = new TreeMap<String, Integer>();
		BufferedReader in = new BufferedReader(new FileReader("data.txt"));
		String line;
		while ((line = in.readLine()) != null) {
			transactions.add(line);
			summary.put(line, summary.getOrDefault(line, 0) + 1);
		}
		in.close();

		System.out.println("Input Summary");
		for (String ss : summary.keySet()) {
			System.out.println(ss + "=" + summary.get(ss));
		}

		DatagramSocket socket = new DatagramSocket();
		/**
		 * For each transaction, the client does the following: 1. Hash the
		 * itemset into IP + port number 2. Send out a request to this address
		 * 3. Busy wait for response (i.e. listening) 4. Print out the response
		 **/

		for (int i = 0; i < transactions.size(); i++) {
			String cur_itemset = transactions.get(i);

			// prepare IP, content
			SocketAddress address = Server.hashing_server(cur_itemset.substring(0, 1));

			StringBuilder content = new StringBuilder();
			content.append("c-");
			content.append(cur_itemset);

			byte[] buf = content.toString().getBytes();
			DatagramPacket packet = new DatagramPacket(buf, buf.length,
					address);

			// send datapackaet
			socket.send(packet);
			System.out.println("Request sent: " + cur_itemset);

			// get response
			packet = new DatagramPacket(buf, buf.length);
			socket.receive(packet);
			
			byte[] data = new byte[packet.getLength()];
			System.arraycopy(packet.getData(), packet.getOffset(), data, 0, packet.getLength());
			
			// display response
			String received = new String(data);
			System.out.println(received);
		}
		socket.close();
	}
}