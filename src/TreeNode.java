import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.TreeMap;

class TreeNode {
	public String reference; // always be one char, e.g. "B"
	public String full_reference; // can be more than 1 char, e.g. "AB"
	public int support;
	TreeMap<String, Integer> combination_table = new TreeMap<String, Integer>();
	TreeMap<Character, Integer> item_table = new TreeMap<Character, Integer>();
	// child notes are sorted alphabetically
	TreeMap<Character, InetSocketAddress> children = new TreeMap<Character, InetSocketAddress>(); // full ref.
	HashSet<Character> invalid_item = new HashSet<Character>();
	
	void send_response(DatagramSocket socket, String msg,
			InetSocketAddress clientAddress) {
		byte[] buf = msg.toString().getBytes();
		DatagramPacket packet = new DatagramPacket(buf, buf.length,
				clientAddress);
		try {
			socket.send(packet);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	void Process(String truncated_transaction, String transaction, InetSocketAddress InetSocketAddress)
			throws SocketException {
		/*
		 * the node first check if the transaction info matches its own: 
		 * if so,
		 * 		1) update support & tables, and 2) send response to client, 
		 * 
		 * if not, neither does it have any child, do the same thing with one more step:
		 * 		3) create new nodes if the support exceeds min_support 
		 * 
		 * if not but it has any child, send truncated transaction to children, but preserve
		 * info in combo-list (except when the transaction starts w/ a child
		 * node ref.)
		 * 
		 * @InetSocketAddress: the address of the client
		 */
		try {
			DatagramSocket socket = new DatagramSocket();
			System.out.printf("[Node %s] processing %s\n",full_reference, transaction);

			if (truncated_transaction.length() == 0) {
				/*
				 * if it is an exact match, update support and send the client a response
				 */
				if (Server.VERBOSE) {
					System.out.printf("[Node %s] transaction=%s: Exact match. Response sent.\n", 
							full_reference, transaction);
				}
				support++;

				// send response to the client
				send_response(socket, "Processed by node " + full_reference,
						InetSocketAddress);
				

			} else if (children.isEmpty()) {
				/* if the node has no child, update support and two tables, send response
				 */
				if (Server.VERBOSE) {
					System.out.printf("[Node %s] transaction=%s: No children. Response sent.\n", 
							full_reference, transaction);
				}
				
				support++;

				// update item_table & combo-table
				update_item_table(truncated_transaction);
				update_combo_table(truncated_transaction);
				
				// when necessary, create a new node and store it
				create_new_node_when_enough_support();

				// send response to the client
				send_response(socket, "Processed by node " + full_reference,
						InetSocketAddress);
			} else {
				if (Server.VERBOSE) {
					System.out.printf("[Node %s] transaction=%s: has child nodes\n",full_reference, transaction);
				}
				support++;
				boolean store_in_combo_list = true;
				int smallest_index = -1;
				for (Character s : children.keySet()) {
					int index = truncated_transaction.indexOf(s);
					if (index >= 0) {
						smallest_index = smallest_index == -1 ? index : Integer.min(smallest_index, index);
						String truncated_to_send = truncated_transaction.substring(index + 1);
						InetSocketAddress childAddr = children.get(s);
						String child_full_ref = full_reference + s;
						System.out.printf("[Node %s] forwarded to node %s\n",
								full_reference, child_full_ref);
						Server.sendRequestToChild(childAddr, truncated_to_send, child_full_ref, InetSocketAddress);

					}
					
					// no need to store in combo list, as the transaction starts w/ child's ref.
					if (index == 0) {
						store_in_combo_list = false;
					}
				}

				if (store_in_combo_list) {
					update_combo_table(truncated_transaction);
				}
				
				// process the result of transaction: store in item-table, create new node if any
				if (smallest_index > 0) {
					String trans_left = truncated_transaction.substring(0,
							smallest_index);
//					System.out.printf("smallest index=%d, trans_left=%s\n",
//							smallest_index, trans_left);
//					print_two_table();
					update_item_table(trans_left);
					create_new_node_when_enough_support();
				}
				
				// send response to the client
				send_response(socket, "Passed on to child by node "
						+ full_reference, InetSocketAddress);
			} 	
			
			socket.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	
	private void update_item_table(String s, int increment) {
		for (int i = 0; i < s.length(); i++) {
			char item = s.charAt(i);
			if (!invalid_item.contains(item)) {
				item_table.put(item, item_table.getOrDefault(item, 0) + increment);	
			}
		}
	}

	private void update_item_table(String s) {
		update_item_table(s, 1);
	}

	private void update_combo_table(String truncated_transaction) {
		if (truncated_transaction.length() > 0) {
			combination_table.put(truncated_transaction, combination_table.getOrDefault(
					truncated_transaction, 0) + 1);
		}
		
	}
	
	private void print_two_table() {
		System.out.println("COMBO TABLE");
		for (String ss : combination_table.keySet()) {
			System.out.print("  " + ss + "=" + combination_table.get(ss));
		}
		System.out.println();
		System.out.println("ITEM TABLE");
		for (Character cc : item_table.keySet()) {
			System.out.print(" " + cc + "=" + item_table.get(cc));
		}
		System.out.println();
	}

	public boolean create_new_node_when_enough_support() {
		boolean newNode = false;
		for (Character key : item_table.keySet()) {
			if (item_table.get(key) >= Server.min_support) {
				InetSocketAddress newNodeAddr = create_new_node(key.charValue());
				children.put(key, newNodeAddr);
				newNode = true;
				// print_two_table();
				if (Server.VERBOSE) {
					System.out.printf("-- New node %s created at server %d\n",
							full_reference + key, newNodeAddr.getPort()
									- Server.PORT_BASE);
				}
				invalid_item.add(key);
			}

		}

		// reset item_table based on combo table
		// System.out.print("before clearing, invalid=: ");
		// for (Character cc : invalid_item) {
		// System.out.print(cc + " ");
		// }
		// System.out.println();
		// System.out.println("Node=" + full_reference + ", before clear:");
		// print_two_table();
		item_table.clear();
		for (String ss : combination_table.keySet()) {
			update_item_table(ss, combination_table.get(ss));

		}

		// System.out.println("Node="+full_reference +
		// ", the end of creating new node when enough:");
		// print_two_table();
		
		return newNode;
	}
	
	private InetSocketAddress create_new_node(char suitor) {
		/*
		 * create new node, and add it to children list
		 */
		// to create a new node, we need to collect item & combo table info
		TreeMap<String, Integer> new_comb_table = new TreeMap<String, Integer>();
		TreeMap<Character, Integer> new_item_table = new TreeMap<Character, Integer>();
		
		HashMap<String, String> combo_update = new HashMap<String, String>();
		for (String key : combination_table.keySet()) {
			int index = key.indexOf(suitor);
			if (index >= 0) {
				String temp_itemset = key.substring(index + 1);
				if (temp_itemset.length() > 0) {
					new_comb_table.put(temp_itemset, combination_table.get(key));
				}
				
				for (int i = 0; i < temp_itemset.length(); i++) {
					char item = temp_itemset.charAt(i);
					new_item_table.put(item,
							new_item_table.getOrDefault(item, 0)
									+ combination_table.get(key));
				}
				
				// prepare to cut strings in combo-list for the current node
				if (index > 0) {
					String remained = key.substring(0, index + 1);
					combo_update.put(key, remained);
				}

			}
		}

		// cut strings in combo list
		for (String s: combo_update.keySet()) {
			int freq = combination_table.remove(s);
			combination_table.put(combo_update.get(s), freq);
		}
		
		// use server handle to create a new node
		InetSocketAddress newNodeAddr = Server.createNewNode(suitor,
				full_reference + Character.toString(suitor), new_comb_table,
				new_item_table);
		return newNodeAddr;
	}

	/*
	 * For a single item (e.g. "B")
	 */
	TreeNode(String ref) {
		this(ref, ref, 0, new TreeMap<String, Integer>(), new TreeMap<Character, Integer>());
		assert (ref.length() == 1);
		item_table.put(ref.charAt(0), 1);
	}

	TreeNode(String ref, String full_ref, int s, TreeMap<String, Integer> c_table, TreeMap<Character, Integer> i_table) {
		reference = ref;
		full_reference = full_ref;
		support = s;
		combination_table = c_table;
		item_table = i_table;
	}

}
