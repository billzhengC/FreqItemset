import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.TreeMap;

public class Server {
	/*
	 * The Server listens on a port (according to node id) and then
	 * 
	 * 1. Client request (prefix "c-"): 
	 *  a. it checks if the transaction can be attached to any subtree
	 * 	b. If no, create a new (and seperate) subtree whose root stores the transaction 
	 * 	c. Call the root to process the transaction (pass along the client's address)
	 *  P.S. the root (or one of its descedents), rather than the server will send return value to the client
	 *  
	 * 2. Node request (prefix "n-"): pass on the info to that node and call its "process" 
	 * 
	 * 3. New node request (prefix "x-"): create a new node in that server
	 * 
	 */

	public static InetAddress ADDR;
	public static int node_id;
	public static int port;
	public static InetSocketAddress server_socket_addr;
	private static TreeMap<String, TreeNode> headNodes; // key = full ref. 
	private static TreeMap<String, TreeNode> allNodes;  // key = full ref. (used when called by other nodes)
	protected static DatagramSocket socket;
	
	public static int N_NODE = 4;
	public static int min_support = 3;
	public static boolean USE_REMOTE = true;
	public static boolean SMART_CLIENT = false;
	public static boolean VERBOSE = false;
	public static int PORT_BASE = 10500;

	public static InetSocketAddress hashing_server(String s) {
		int p = PORT_BASE + s.hashCode() % N_NODE;
		return new InetSocketAddress("127.0.0.1", p);
	}

	// paras: node_id (1 to N_NODE)
	public static void main(String[] args) {
		node_id = Integer.parseInt(args[0]);
		try {
			headNodes = new TreeMap<String, TreeNode>();
			allNodes = new TreeMap<String, TreeNode>();
			// create socket t
			ADDR = InetAddress.getByName("127.0.0.1");
			port = 10500 + node_id;
			server_socket_addr = new InetSocketAddress(ADDR, port);
			try {
				socket = new DatagramSocket(server_socket_addr);
			} catch (SocketException e) {
				e.printStackTrace();
			}
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}

		System.out.printf("[Server %d] is listening on port %d. Min support=%d\n", node_id, port, min_support);
		while (true) {
			try {
				// receive request
				byte[] buf = new byte[512];
				DatagramPacket packet = new DatagramPacket(buf, buf.length);
				
				socket.receive(packet); // this is BLOCKING!
				byte[] data = new byte[packet.getLength()];
				System.arraycopy(packet.getData(), packet.getOffset(), data, 0, packet.getLength());
				String request = new String(data);

				// figure out response
				InetSocketAddress clientAddr = new InetSocketAddress(packet.getAddress(), packet.getPort());
				String request_content = request.substring(2);
				if (request.charAt(0) == 'c') {
					// client request
					// mode 1: normal client (hash the first char)
					// mode 2: smart client (
					System.out.printf("\n[Server %d] Client request: %s\n",
							node_id, request_content);
					TreeNode node_handle = null;
					if (SMART_CLIENT) {
						// TODO: smart client
					} else {
						String head_str = request_content.substring(0, 1);
						if (headNodes.containsKey(head_str)) {
							node_handle = headNodes.get(head_str);
						} else {
							node_handle = new TreeNode(head_str);
							headNodes.put(head_str, node_handle);
							allNodes.put(head_str, node_handle);
							System.out.printf("[Server %d] New node created: %s\n",
									node_id, request_content.substring(0, 1)); // this is head node
						}
						node_handle.Process(request_content.substring(1), request_content, 
								clientAddr);
					}
					
				} else if (request.charAt(0) == 'n') {
					// node request
					// System.out.println("request content: " +
					// request_content);
					String[] tokens = request_content.split("-");
					String truncated_transaction = tokens[0];
					String child_full_ref = tokens[1];
					int child_port = Integer.parseInt(tokens[2]);

					TreeNode child_node_handle = allNodes.get(child_full_ref);
					child_node_handle.Process(truncated_transaction,
							child_full_ref + truncated_transaction,
							new InetSocketAddress("127.0.0.1", child_port));
					System.out.printf("[Server %d] Node request received: transaction (%s) forwarded to child node %s\n", 
							node_id, truncated_transaction, child_full_ref);

				} else if (request.charAt(0) == 'x') {
					// create a new Node
					// add to HeadNode list
					String[] tokens = request_content.split("-");
					assert (tokens.length % 2 == 1);
					String ful_ref = tokens[0];
					TreeMap<Character, Integer> i_table = new TreeMap<Character, Integer>();
					TreeMap<String, Integer> c_table = new TreeMap<String, Integer>();
					for (int i = 1; i < tokens.length; i = i + 2) {
						String combo = tokens[i];
						int freq = Integer.parseInt(tokens[i + 1]);
						c_table.put(combo, freq);
						for (int j = 0; j < combo.length(); j++) {
							char item = combo.charAt(j);
							i_table.put(item, i_table.getOrDefault(item, 0) + 1);
						}
					}
					TreeNode tn = new TreeNode(ful_ref.substring(ful_ref.length() -1), 
							ful_ref, min_support, c_table, i_table);
					headNodes.put(ful_ref, tn);
					allNodes.put(ful_ref, tn);
					
					System.out.printf("[Server %d] A new node is created: %s\n", node_id, ful_ref);

					tn.create_new_node_when_enough_support();
				}
				
				
				// send the response to the client
				send_response("Request acknowledged by server " + node_id, clientAddr);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		// socket.close();
		
    }
	
	public static void send_response(String msg, InetSocketAddress clientAddress) {
		byte[] buf = new byte[512];
		buf = msg.toString().getBytes();
		DatagramPacket packet = new DatagramPacket(buf, buf.length,
				clientAddress);
		try {
			socket.send(packet);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static InetSocketAddress createNewNode(char suitor,
			String full_ref_of_node,
			TreeMap<String, Integer> new_comb_table,
			TreeMap<Character, Integer> new_item_table) {
		// option 1: always create locally (return the addr of itself)
		// option 2: use hash to create a (possibly) remote node
		
		InetSocketAddress newNodeAddr = null;
		if (USE_REMOTE) {
			// get node addr via hashing
			newNodeAddr = hashing_server(full_ref_of_node);
			if (newNodeAddr.getPort() == port) {
				TreeNode newNode = new TreeNode(suitor + "", full_ref_of_node, min_support, new_comb_table, new_item_table);
				allNodes.put(full_ref_of_node, newNode);
				
				System.out.printf("[Server %d] A new node is created: %s\n", node_id, full_ref_of_node);
				newNode.create_new_node_when_enough_support();
				
			} else {
				// craft message
				StringBuilder sb = new StringBuilder();
				sb.append("x-");
				sb.append(full_ref_of_node + "-");
				for (String s : new_comb_table.keySet()) {
					sb.append(s);
					sb.append("-");
					sb.append(new_comb_table.get(s));
					sb.append("-");
				}
				sb.deleteCharAt(sb.length() - 1);
				sendMsg(sb.toString(), newNodeAddr);
			}
		} else {
			newNodeAddr = server_socket_addr;
			TreeNode newNode = new TreeNode(suitor + "", full_ref_of_node, min_support, new_comb_table, new_item_table);
			allNodes.put(full_ref_of_node, newNode);
		}
		
		return newNodeAddr;
	}

	public static void sendRequestToChild(InetSocketAddress childAddr,
			String truncated_to_send, String child_full_ref,
			InetSocketAddress clientAddress) {
		/*
		 * forward "process" request to a child which may live in another server
		 */
		if (childAddr.getPort() == port) {
			// can find the child locally
			TreeNode nn = allNodes.get(child_full_ref);
			try {
				nn.Process(truncated_to_send, child_full_ref + truncated_to_send, clientAddress);
			} catch (SocketException e) {
				e.printStackTrace();
			}
			
		} else {
			// craft message and send it
			StringBuilder sb = new StringBuilder();
			sb.append("n-");
			sb.append(truncated_to_send);
			sb.append("-");
			sb.append(child_full_ref);
			sb.append("-");
			sb.append(clientAddress.getPort());
			sendMsg(sb.toString(), childAddr);
		}
	}
	
	private static void sendMsg(String msg, InetSocketAddress destination) {
		byte[] buf = msg.toString().getBytes();
		DatagramPacket packet = new DatagramPacket(buf, buf.length, destination);
		try {
			socket.send(packet);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}