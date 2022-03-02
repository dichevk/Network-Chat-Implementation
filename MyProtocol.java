package my_code;

import client.*;

import java.nio.ByteBuffer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;

/**
 * This is just some example code to show you how to interact with the server
 * using the provided client and two queues. Feel free to modify this code in
 * any way you like!
 */

public class MyProtocol {

	// The host to connect to. Set this to localhost when using the audio interface
	// tool.
	private static String SERVER_IP = "netsys2.ewi.utwente.nl"; // "127.0.0.1";
	// The port to connect to. 8954 for the simulation server.
	private static int SERVER_PORT = 8954;
	// The frequency to use.
	private static int frequency = 21100; // TODO: Set this to your group frequency!

	neighbourTrackerThread neighbourTracker;
	messageHandlerThread messageHandler;
	private byte clientId; // Client ID
	private byte seqNum;
	private BlockingQueue<Message> receivedQueue;
	private BlockingQueue<Message> sendingQueue;
	private MessageType linkStatus;
	boolean isSending;
	boolean sendingSuccessful;

	public MyProtocol(String server_ip, int server_port, int frequency, byte cId) {
		receivedQueue = new LinkedBlockingQueue<Message>();
		sendingQueue = new LinkedBlockingQueue<Message>();
		neighbourTracker = new neighbourTrackerThread();
		messageHandler = new messageHandlerThread();
		clientId = cId;
		seqNum = 0;

		new receiveThread(receivedQueue).start(); // Start thread to handle received messages!
		messageHandler.start();
		new echoThread(sendingQueue).start();
		System.out.println("Setting up, please wait.");
		try {
			Thread.sleep(1000);
			new Client(SERVER_IP, SERVER_PORT, frequency, receivedQueue, sendingQueue); // Give the client the Queues to
																						// use

			// handle sending from stdin from this thread.

			ByteBuffer temp = ByteBuffer.allocate(1024);
			int read = 0;
			while (true) {
				read = System.in.read(temp.array()); // Get data from stdin, hit enter to send!
				if (read > 0) {
					Message msg;
					if (read > 27) {
						int offSet = 0;
						int lastSpaceFound = -1;
						int prevSpaceFound = -1;
						int i = 0;
						int j = 100;
						while (offSet != read - 2) {
							if ((read - 2) - offSet < 27) {
								j = (read - 2) - offSet;
								for (i = 0; i < offSet + j; i++) {
									if (temp.array()[i] == (byte) ' ') {
										lastSpaceFound = i;
									}
								}
							} else {
								for (i = 0; i < offSet + 27; i++) {
									if (temp.array()[i] == (byte) ' ') {
										lastSpaceFound = i;
									}
								}
							}
							if (j < 27) {
								ByteBuffer toSend = ByteBuffer.allocate(j + 5);
								toSend.put(clientId);
								toSend.put((byte) 0);
								if (seqNum < 255) {
									toSend.put(seqNum);
									seqNum++;
								} else {
									seqNum = 0;
									toSend.put(seqNum);
								}
								toSend.put(clientId);
								toSend.put((byte) (j));
								toSend.put(temp.array(), offSet, j);
								offSet += j;
								msg = new Message(MessageType.DATA, toSend);
								// this.mac();
								this.sendData(msg);
								messageHandler.addUnackMsg(toSend, clientId);
							} else {
								ByteBuffer toSend = ByteBuffer.allocate(32);
								if (lastSpaceFound == -1 || lastSpaceFound == prevSpaceFound) {
									toSend.put(clientId);
									toSend.put((byte) 0);
									if (seqNum < 255) {
										toSend.put(seqNum);
										seqNum++;
									} else {
										seqNum = 0;
										toSend.put(seqNum);
									}
									toSend.put(clientId);
									toSend.put((byte) (27));
									toSend.put(temp.array(), offSet, 27);
									offSet += 27;
									i = offSet;
									msg = new Message(MessageType.DATA, toSend);
									// this.mac();
									this.sendData(msg);
									messageHandler.addUnackMsg(toSend, clientId);
								} else {
									toSend.put(clientId);
									toSend.put((byte) 0);
									if (seqNum < 255) {
										toSend.put(seqNum);
										seqNum++;
									} else {
										seqNum = 0;
										toSend.put(seqNum);
									}
									toSend.put(clientId);
									toSend.put((byte) (lastSpaceFound - offSet));
									toSend.put(temp.array(), offSet, lastSpaceFound - offSet);
									prevSpaceFound = lastSpaceFound;
									offSet = lastSpaceFound + 1;
									i = offSet;
									msg = new Message(MessageType.DATA, toSend);
									// this.mac();
									this.sendData(msg);
									messageHandler.addUnackMsg(toSend, clientId);
								}
							}
						}

					} else {
						ByteBuffer toSend = ByteBuffer.allocate(read + 3); // jave includes newlines in System.in.read,
						toSend.put(clientId);
						toSend.put((byte) 0);
						if (seqNum < 255) {
							toSend.put(seqNum);
							seqNum++;
						} else {
							seqNum = 0;
							toSend.put(seqNum);
						} // so -2 to ignore this
						toSend.put(clientId);
						toSend.put((byte) (read - 2));
						toSend.put(temp.array(), 0, read - 2); // jave includes newlines in System.in.read, so -2 to
																// ignore this
						msg = new Message(MessageType.DATA, toSend);
						// this.mac();
						this.sendData(msg);
						messageHandler.addUnackMsg(toSend, clientId);
					}
				}
			}
		} catch (InterruptedException e) {
			System.exit(2);
		} catch (IOException e) {
			System.exit(2);
		}
	}

	public boolean detectCollision() {
		if (linkStatus == MessageType.BUSY && isSending == false) {
			try {
				Thread.sleep(new Random().nextInt(500));
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			return true;
		} else {
			return false;
		}
	}

	public void sendData(Message msg) {
		do {
			try {
				mac();
				sendingQueue.put(msg);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		} while (detectCollision());
	}

	public static void main(String args[]) {
		Scanner scn = new Scanner(System.in);
		System.out.print("Please enter your ID: ");
		byte cId = scn.nextByte();
		new MyProtocol(SERVER_IP, SERVER_PORT, frequency, cId);
	}

	private class receiveThread extends Thread {
		private BlockingQueue<Message> receivedQueue;

		public receiveThread(BlockingQueue<Message> receivedQueue) {
			super();
			this.receivedQueue = receivedQueue;
		}

		public void printByteBuffer(ByteBuffer bytes, int bytesLength) {
			String result = "";
			if (bytes.get(3) != 0) {
				for (int i = 5; i < 5 + (int) bytes.get(4); i++) {
					result += (char) bytes.get(i);
				}
			}
			System.out.print(result);
			System.out.println();
		}

		public void run() {
			while (true) {
				try {
					Message m = receivedQueue.take();
					if (m.getType() == MessageType.BUSY) {
						linkStatus = MessageType.BUSY;
						System.out.println("BUSY");
					} else if (m.getType() == MessageType.FREE) {
						linkStatus = MessageType.FREE;
						System.out.println("FREE");
					} else if (m.getType() == MessageType.DATA) {
						neighbourTracker.addNeighbour(m.getData().get(0));
						if (neighbourTracker.isAlive() == false) {
							neighbourTracker.start();
						}
						if ((m.getData().get(1) != (byte) 1)) {
							if ((!messageHandler.isReceived(m.getData()))) {
								System.out.print(String.format("User %s : ", m.getData().get(3)));
								printByteBuffer(m.getData(), m.getData().capacity()); // Just print the data
							} else {
								messageHandler.removePacket(m.getData());
							}
						}
					} else if (m.getType() == MessageType.DATA_SHORT) {
						System.out.print("DATA_SHORT: ");
						printByteBuffer(m.getData(), m.getData().capacity()); // Just print the data
					} else if (m.getType() == MessageType.DONE_SENDING) {
						isSending = false;
						System.out.println("DONE_SENDING");
					} else if (m.getType() == MessageType.HELLO) {
						System.out.println("HELLO");
					} else if (m.getType() == MessageType.SENDING) {
						isSending = true;
						System.out.println("SENDING");
					} else if (m.getType() == MessageType.END) {
						System.out.println("END");
						System.exit(0);
					}
				} catch (InterruptedException e) {
					System.err.println("Failed to take from queue: " + e);
				}
			}
		}
	}

	private class echoThread extends Thread {
		private BlockingQueue<Message> sendingQueue;

		public echoThread(BlockingQueue<Message> sendingQueue) {
			super();
			this.sendingQueue = sendingQueue;
		}

		public void sendEchoMessage() {
			ByteBuffer echoMsg = ByteBuffer.allocate(5);
			echoMsg.put(clientId);
			echoMsg.put((byte) 1);
			echoMsg.put((byte) 0);
			echoMsg.put((byte) clientId);
			echoMsg.put((byte) 0);
			// mac();
			sendData(new Message(MessageType.DATA, echoMsg));
			System.out.println("It's an echoOoOoOo");
		}

		public void run() {
			try {
				Thread.sleep(new Random().nextInt(300) + (clientId * new Random().nextInt(40)));
				while (true) {
					sendEchoMessage();
					Thread.sleep(50000 + (new Random().nextInt(300) + (clientId * new Random().nextInt(40))));
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	private class neighbourTrackerThread extends Thread {
		private ConcurrentHashMap<Byte, Integer> neighbours;
		private ConcurrentHashMap<Byte, Integer> latestEchoesReceived;
		private ArrayList<Byte> neighboursToSend;
		int TTL = 2;

		public neighbourTrackerThread() {
			super();
			neighbours = new ConcurrentHashMap<Byte, Integer>(4);
			latestEchoesReceived = new ConcurrentHashMap<Byte, Integer>(4);
			neighboursToSend = new ArrayList<Byte>();
		}

		public void update() {
			synchronized (neighboursToSend) {
				neighboursToSend.clear();
			}
			int ttl = 0;
			if (neighbours.isEmpty() == false) {
				for (Byte cId : neighbours.keySet()) {
					ttl = neighbours.get(cId);
					if ((ttl - 1) == 0) {
						neighbours.remove(cId);
					} else {
						ttl -= 1;
						neighbours.put(cId, ttl);
					}
				}
			}
			synchronized (latestEchoesReceived) {
				if (latestEchoesReceived.isEmpty() == false) {
					for (Byte cId : latestEchoesReceived.keySet()) {
						neighbours.put(cId, TTL);
					}
					latestEchoesReceived.clear();
				}
			}
			synchronized (neighboursToSend) {
				if (neighbours.isEmpty() == false) {
					for (Byte neightbour : neighbours.keySet()) {
						neighboursToSend.add(neightbour);
					}
					messageHandler.updateNeighbours(neighboursToSend);
					System.out.println("Neighbours :" + neighboursToSend);
				} else {
					messageHandler.updateNeighbours(-1);
					System.out.println("no neighbours detected");
				}
			}
		}

		public void addNeighbour(Byte cId) {
			synchronized (latestEchoesReceived) {
				latestEchoesReceived.put(cId, TTL);
			}
		}

		public void run() {
			while (true) {
				try {
					this.update();
					Thread.sleep(50000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}

	}

	private class messageHandlerThread extends Thread {
		private BlockingQueue<Packet> unacknowledgedMsgs;
		private ConcurrentHashMap<Byte, ConcurrentHashMap<Byte, ByteBuffer>> receivedMsgs;
		private BlockingQueue<Packet> toAcknowledge;
		private ArrayList<Byte> neighbours;
		private ConcurrentHashMap<Packet, ArrayList<Byte>> ackTracker;
		// private ConcurrentHashMap<Byte, ConcurrentHashMap<Byte, Integer>>
		// lostPackets;

		public messageHandlerThread() {
			super();
			unacknowledgedMsgs = new LinkedBlockingQueue<Packet>();
			receivedMsgs = new ConcurrentHashMap<Byte, ConcurrentHashMap<Byte, ByteBuffer>>();
			toAcknowledge = new LinkedBlockingQueue<Packet>();
			neighbours = new ArrayList<Byte>();
			ackTracker = new ConcurrentHashMap<Packet, ArrayList<Byte>>();
			// lostPackets = new ConcurrentHashMap<Byte, ConcurrentHashMap<Byte,
			// Integer>>();
		}

		public void updateNeighbours(ArrayList<Byte> updated) {
			synchronized (ackTracker) {
				for (Packet pkt : ackTracker.keySet()) {
					for (Byte neighbour : ackTracker.get(pkt)) {
						synchronized (updated) {
							if (!updated.contains(neighbour)) {
								if (ackTracker.get(pkt).size() > 1) {
									ackTracker.get(pkt).remove(neighbour);
								} else {
									ackTracker.remove(pkt);
									unacknowledgedMsgs.remove(pkt);
								}
							}
						}
					}
				}
			}
			for (Packet pkt : toAcknowledge) {
				if (!updated.contains(pkt.origin)) {
					toAcknowledge.remove(pkt);
				}
			}
			synchronized (neighbours) {
				neighbours.clear();
				for (Byte n : updated) {
					neighbours.add(n);
				}
			}
		}

		public void updateNeighbours(int empty) {
			unacknowledgedMsgs.clear();
			toAcknowledge.clear();
			synchronized (neighbours) {
				neighbours.clear();
			}
		}

		public void addUnackMsg(ByteBuffer data) {
			Packet pkt = new Packet(data.get(3), data.get(2), data);
			unacknowledgedMsgs.add(pkt);
			System.out.println("Expected nodes to ACK origin: " + pkt.origin() + " /seqNum: " + pkt.seqNum() + " - "
					+ ackTracker.get(pkt));
		}

		public void addUnackMsg(ByteBuffer data, byte sender) {
			boolean moreThanOne;
			Packet pkt = new Packet(data.get(3), data.get(2), data);
			boolean isContained = false;
			synchronized (ackTracker) {
				if (!ackTracker.isEmpty()) {
					for (Packet packet : ackTracker.keySet()) {
						if ((packet.origin == pkt.origin) && (packet.seqNum == pkt.seqNum)) {
							isContained = true;
							break;
						}
					}
				}
			}
			synchronized (neighbours) {
				moreThanOne = neighbours.size() > 1;
			}
			if (moreThanOne) {
				if (isContained == false) {
					unacknowledgedMsgs.add(pkt);
					synchronized (ackTracker) {
						ackTracker.put(pkt, new ArrayList<Byte>());
						synchronized (neighbours) {
							for (Byte neighbour : neighbours) {
								if (neighbour != sender) {
									ackTracker.get(pkt).add(neighbour);
								}
							}
						}
					}
				}
			} else {
				synchronized (neighbours) {
					if (neighbours.size() == 1) {
						if (neighbours.get(0) != sender) {
							synchronized (ackTracker) {
								ackTracker.put(pkt, new ArrayList<Byte>());
								ackTracker.get(pkt).add(neighbours.get(0));
							}
						}
					}
				}
			}
		}

		public void removePacket(ByteBuffer data) {
			Packet pkt = new Packet(data.get(3), data.get(2), data);
			boolean isContained = false;
			synchronized (ackTracker) {
				if (!ackTracker.isEmpty()) {
					for (Packet packet : ackTracker.keySet()) {
						if ((packet.origin == pkt.origin) && (packet.seqNum == pkt.seqNum)) {
							pkt = packet;
							isContained = true;
							break;
						}
					}
				}
				if (isContained == true) {
					if (ackTracker.get(pkt).size() > 1) {
						if (ackTracker.get(pkt).contains(data.get(0))) {
							ackTracker.get(pkt).remove((Byte) data.get(0)); // U MAKE ME WORRY
						}
					} else {
						ackTracker.remove(pkt);
						unacknowledgedMsgs.remove(pkt);
					}
				}
			}
		}

		public Boolean isReceived(ByteBuffer data) {
			// int maxAllowedRetrans = neighbours.size() + 1;
			synchronized (receivedMsgs) {
				Packet pkt = new Packet(data.get(3), data.get(2), data);
				if (data.get(3) != clientId) {
					if (receivedMsgs.containsKey(data.get(3))) {
						if (receivedMsgs.get(data.get(3)).containsKey(data.get(2))) {
							/*
							 * if(lostPackets.get(data.get(3)).get(data.get(2)) <= maxAllowedRetrans) { int
							 * currentCount = lostPackets.get(data.get(3)).get(data.get(2)); currentCount++;
							 * lostPackets.get(data.get(3)).put(data.get(2), currentCount); }else {
							 * lostPackets.get(data.get(3)).remove(data.get(2));
							 * receivedMsgs.get(data.get(3)).remove(data.get(2)); }
							 */
							return true;
						} else {
							// lostPackets.get(data.get(3)).put(data.get(2), 1);
							receivedMsgs.get(data.get(3)).put(data.get(2), data);
							synchronized (toAcknowledge) {
								toAcknowledge.add(pkt);
							}
							return false;
						}
					} else {
						receivedMsgs.put(data.get(3), new ConcurrentHashMap<Byte, ByteBuffer>());
						receivedMsgs.get(data.get(3)).put(data.get(2), data);
						// lostPackets.put(data.get(3), new ConcurrentHashMap<Byte, Integer>());
						// lostPackets.get(data.get(3)).put(data.get(2), 1);
						synchronized (toAcknowledge) {
							toAcknowledge.add(pkt);
						}
						return false;
					}
				}
			}
			return true;
		}

		public void run() {
			long timeOut = 0;
			long beginnig = 0;
			long end = 0;
			ByteBuffer ackMsg;
			Message msg;
			Packet pkt;
			while (true) {
				timeOut += end - beginnig;
				beginnig = System.currentTimeMillis();
				if (timeOut > 25000) {
					if (!unacknowledgedMsgs.isEmpty()) {
						try {
							int size = unacknowledgedMsgs.size();
							for (int i = 0; i < size; i++) {
								pkt = unacknowledgedMsgs.take();
								msg = new Message(MessageType.DATA, pkt.data());
								sendData(msg);
								addUnackMsg(pkt.data);
								System.out.println(
										"Retransmission - origin: " + pkt.origin() + " /seqNum: " + pkt.seqNum());
							}
							timeOut = 0;
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
				}
				if (!toAcknowledge.isEmpty()) {
					try {
						pkt = toAcknowledge.take();
						ackMsg = ByteBuffer.allocate(pkt.data.get(4) + 5);
						ackMsg.put(clientId);
						ackMsg.put((byte) 2);
						ackMsg.put(pkt.seqNum());
						ackMsg.put(pkt.origin());
						ackMsg.put(pkt.data.get(4));
						ackMsg.put(pkt.data().array(), 5, pkt.data().get(4));
						msg = new Message(MessageType.DATA, ackMsg);
						// mac();
						sendData(msg);
						this.addUnackMsg(msg.getData(), pkt.data.get(0));
						System.out.println("Ack - origin: " + pkt.data.get(3) + " /seqNum : " + pkt.data.get(2));
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				end = System.currentTimeMillis();
				Thread.yield();
			}
		}

		private class Packet {
			private byte origin;
			private byte seqNum;
			private ByteBuffer data;

			public Packet(byte origin, byte seqNum, ByteBuffer data) {
				this.origin = origin;
				this.seqNum = seqNum;
				this.data = data;
			}

			public byte origin() {
				return this.origin;
			}

			public byte seqNum() {
				return this.seqNum;
			}

			public ByteBuffer data() {
				return this.data;
			}
		}
	}

	public void mac() {
		try {
			if (linkStatus == MessageType.BUSY) {
				if (isSending == true) {
					return;
				}
				Thread.sleep(new Random().nextInt(500));
				while (linkStatus == MessageType.BUSY) {
					Thread.sleep(new Random().nextInt(500));
				}
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
