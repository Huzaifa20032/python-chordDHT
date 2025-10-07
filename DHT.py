import socket 
import threading
import os
import time
import hashlib
import json

class Node:
	def __init__(self, host, port):
		self.stop = False
		self.host = host
		self.port = port
		self.M = 16
		self.N = 2**self.M
		self.key = self.hasher(host+str(port))
		# You will need to kill this thread when leaving, to do so just set self.stop = True
		threading.Thread(target = self.listener).start()
		self.files = []
		self.backUpFiles = []
		if not os.path.exists(host+"_"+str(port)):
			os.mkdir(host+"_"+str(port))
		'''
		------------------------------------------------------------------------------------
		DO NOT EDIT ANYTHING ABOVE THIS LINE
		'''
		# Set value of the following variables appropriately to pass Intialization test
		self.successor = ('localhost', port)
		self.predecessor = ('localhost', port)
		self.succsucc = ('localhost', port)
		# additional state variables
		self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.sock2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.sock3 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.sock4 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.pingingSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.pingResult = True
		threading.Thread(target = self.ping).start()


	def hasher(self, key):
		'''
		DO NOT EDIT THIS FUNCTION.
		You can use this function as follow:
			For a node: self.hasher(node.host+str(node.port))
			For a file: self.hasher(file)
		'''
		return int(hashlib.md5(key.encode()).hexdigest(), 16) % self.N


	def handleConnection(self, client, addr):
		'''
		 Function to handle each inbound connection, called as a thread from the listener.
		'''
		recvdData = client.recv(5000).decode('utf-8')

		signal, data = self.parseMsg(recvdData)

		if signal == "ping":
			client.send(("pingReturn," + json.dumps(self.successor)).encode('utf-8'))
			# close from other end
   
		elif signal == "join":
			toLookup = tuple(json.loads(data))
			if (self.host, self.port) == self.successor:
				self.predecessor = toLookup
				self.successor = toLookup
				client.send(("joinAnsEdge, ").encode('utf-8'))
				return
			else:
				new_hash = self.hasher(toLookup[0]+str(toLookup[1]))
				ansLookup = self.lookupSuccessor(new_hash)
				ansLookupSending = json.dumps(ansLookup)
				client.send(("joinAns," + ansLookupSending).encode('utf-8'))
				# close from other end

		elif signal == "updatePred":
			newPred = tuple(json.loads(data))
			client.send(("returnUpdatePred," + json.dumps(self.predecessor)).encode('utf-8'))
			self.predecessor = newPred
			self.redistributeFilesJoin()
		
		elif signal == "updateSucc":
			newSucc = tuple(json.loads(data))
			client.send(("returnUpdateSucc," + json.dumps(self.successor)).encode('utf-8'))
			self.successor = newSucc

		elif signal == "lookup":
			new_hash = int(data)
			ansLookup = self.lookupSuccessor(new_hash)
			client.send(("lookupAns," + json.dumps(ansLookup)).encode('utf-8'))

		elif signal == "sendFile":
			fileNameToReceive = data
			client.send(("ok," + json.dumps(self.successor)).encode('utf-8'))
			self.recieveFile(client, self.host + "_" + str(self.port) + "/" + (fileNameToReceive)) # receives first
			# open the file
			self.files.append(fileNameToReceive)

			self.sock4.connect(self.successor)
			self.sock4.send(("sendFileBackup," + fileNameToReceive).encode('utf-8'))
			recvData = self.sock4.recv(1000)
			self.sendFile(self.sock4, self.host + "_" + str(self.port) + "/" + fileNameToReceive)
			self.sock4.close()
			self.sock4 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

		elif signal == "sendFileBackup":
			fileNameToReceive = data
			client.send("ok, ".encode('utf-8'))
			self.recieveFile(client, self.host + "_" + str(self.port) + "/" + (fileNameToReceive)) # receives first
			# open the file
			self.backUpFiles.append(fileNameToReceive)


		elif signal == "checkFile":
			fileNameToCheck = data
			if fileNameToCheck in self.files:
				client.send(("checkFileResult," + "found").encode('utf-8'))
			else:
				client.send(("checkFileResult," + "notfound").encode('utf-8'))

		elif signal == "updatePredecessor":
			newSucc = tuple(json.loads(data))
			self.successor = newSucc
			client.send("ok,".encode('utf-8'))
		
		elif signal == "updateSuccessor":
			newPred = tuple(json.loads(data))
			self.predecessor = newPred
			client.send("ok,".encode('utf-8'))

		elif signal == "pingKilled":
			print("Updated predecessor from: ", self.predecessor, " to: ", tuple(json.loads(data)))
			self.predecessor = tuple(json.loads(data))
			for backupFile in self.backUpFiles:
				self.files.append(backupFile)
			self.backUpFiles = []
			client.send("ok,".encode())


	def listener(self):
		'''
		We have already created a listener for you, any connection made by other nodes will be accepted here.
		For every inbound connection we spin a new thread in the form of handleConnection function. You do not need
		to edit this function. If needed you can edit signature of handleConnection function, but nothing more.
		'''
		listener = socket.socket()
		listener.bind((self.host, self.port))
		listener.listen(10)
		while not self.stop:
			client, addr = listener.accept()
			threading.Thread(target = self.handleConnection, args = (client, addr)).start()
		print ("Shutting down node:", self.host, self.port)
		try:
			listener.shutdown(2)
			listener.close()
		except:
			listener.close()


	# make a lookup function (finds where file needs to be put)
	def lookupSuccessor(self, new_hash):
		# edge case: 2nd node entering
		
		curr_hash = self.hasher(self.host+str(self.port))
		succ_hash = self.hasher(self.successor[0]+str(self.successor[1]))
		pred_hash = self.hasher(self.predecessor[0]+str(self.predecessor[1]))

		if (pred_hash == succ_hash):
			if (curr_hash < succ_hash):
				if (curr_hash < new_hash < succ_hash):
					return (self.successor[0], self.successor[1])
				else:
					return (self.host, self.port)

		elif (curr_hash < new_hash < succ_hash or (new_hash < succ_hash < pred_hash < curr_hash) or (succ_hash < pred_hash < curr_hash < new_hash)):
			# the current node is the successor, return it
			return (self.successor[0], self.successor[1])
			
		# finds the node that is the successor
		self.sock3.connect(self.successor)
		self.sock3.send(("lookup," + str(new_hash)).encode('utf-8'))
		recvData = self.sock3.recv(1000).decode('utf-8')
		self.sock3.close()
		self.sock3 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		signal, data = self.parseMsg(recvData)
		if signal == "lookupAns":
			return tuple(json.loads(data))
		else:
			print("Failed lookup")
			return


	def join(self, joiningAddr):
		'''
		This function handles the logic of a node joining. This function should do a lot of things such as:
		Update successor, predecessor, getting files, back up files. SEE MANUAL FOR DETAILS.
		'''
		# joiningAddr is a tuple ("host", put)
		# if joining node in the first node in the network, then tuple is none

		if joiningAddr == "":
			return

		myCurrAddr = json.dumps((self.host, self.port))
		self.sock2.connect(joiningAddr)
		self.sock2.send(("join," + myCurrAddr).encode('utf-8'))
		recvData = self.sock2.recv(1000).decode('utf-8')
		self.sock2.close()
		self.sock2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		signal, data = self.parseMsg(recvData)
		if signal == "joinAnsEdge":
			self.successor = joiningAddr
			self.predecessor = joiningAddr
			return

		self.successor = tuple(json.loads(data))
		self.sock2.connect(self.successor)
		self.sock2.send(("updatePred," + json.dumps((self.host, self.port))).encode('utf-8'))
		recvdData = self.sock2.recv(1000).decode('utf-8')
		signal, data = self.parseMsg(recvdData)
		self.predecessor = tuple(json.loads(data))
		self.sock2.close()
		self.sock2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

		self.sock2.connect(self.predecessor)
		self.sock2.send(("updateSucc," + json.dumps((self.host, self.port))).encode('utf-8'))
		data = self.sock2.recv(1000).decode('utf-8')
		self.sock2.close()
		self.sock2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

		return
  
		# rehashing of files and backupFiles is required when a new node joins the network



	def put(self, fileName, type = 0):
		'''
		This function should first find node responsible for the file given by fileName, then send the file over the socket to that node
		Responsible node should then replicate the file on appropriate node. SEE MANUAL FOR DETAILS. Responsible node should save the files
		in directory given by host_port e.g. "localhost_20007/file.py".
		'''
		# implement fault tolerant file management
		# sendFile and receiveFile can be used to send and receive files over a socket
		succ = self.lookupSuccessor(self.hasher(fileName))
		self.sock.connect(succ)
		self.sock.send(("sendFile," + fileName).encode('utf-8'))
		recvData = self.sock.recv(1000).decode('utf-8')
		if type == 0:
			self.sendFile(self.sock, fileName) # sends first
		else:
			self.sendFile(self.sock, self.host + "_" + str(self.port) + "/" + fileName) # sends first
		self.sock.close()
		self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

		time.sleep(0.2)

		
	def get(self, fileName):
		'''
		This function finds node responsible for file given by fileName, gets the file from responsible node, saves it in current directory
		i.e. "./file.py" and returns the name of file. If the file is not present on the network, return None.
		'''
		# implement fault tolerant file management
		# every node is responsible for the hashes (and hence files) between its predecessor and itself
		succ = self.lookupSuccessor(self.hasher(fileName))
		self.sock.connect(succ)
		self.sock.send(("checkFile," + fileName).encode('utf-8'))
		recvData = self.sock.recv(1000).decode('utf-8')
		signal, data = self.parseMsg(recvData)
		self.sock.close()
		self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		if data == "found":
			return fileName
		else:
			return

	def leave(self):
		'''
		When called leave, a node should gracefully leave the network i.e. it should update its predecessor that it is leaving
		it should send its share of file to the new responsible node, close all the threads and leave. You can close listener thread
		by setting self.stop flag to True
		'''
		# gracefully leave the network
		self.sock.connect(self.successor)
		self.sock.send(("updateSuccessor," + json.dumps(self.predecessor)).encode('utf-8'))
		self.sock.recv(100)
		self.sock.close()
		self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

		self.sock.connect(self.predecessor)
		self.sock.send(("updatePredecessor," + json.dumps(self.successor)).encode('utf-8'))
		self.sock.recv(100)
		self.sock.close()
		self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

		# send files to corresponding nodes
		time.sleep(0.2)
		for file in self.files:
			self.put(file, 1)

		self.stop = True
		



	def sendFile(self, soc, fileName):
		''' 
		Utility function to send a file over a socket
			Arguments:	soc => a socket object
						fileName => file's name including its path e.g. NetCen/PA3/file.py
		'''
		fileSize = os.path.getsize(fileName)
		soc.send(str(fileSize).encode('utf-8'))
		soc.recv(1024).decode('utf-8')
		with open(fileName, "rb") as file:
			contentChunk = file.read(1024)
			while contentChunk!="".encode('utf-8'):
				soc.send(contentChunk)
				contentChunk = file.read(1024)

	def recieveFile(self, soc, fileName):
		'''
		Utility function to recieve a file over a socket
			Arguments:	soc => a socket object
						fileName => file's name including its path e.g. NetCen/PA3/file.py
		'''
		fileSize = int(soc.recv(1024).decode('utf-8'))
		soc.send("ok".encode('utf-8'))
		contentRecieved = 0
		file = open(fileName, "wb")
		x = 0
		while contentRecieved < fileSize and x < 10:
			x += 1
			contentChunk = soc.recv(1024)
			contentRecieved += len(contentChunk)
			file.write(contentChunk)
		file.close()

	def kill(self):
		# DO NOT EDIT THIS, used for code testing
		self.stop = True

	# Helper functions:
	def ping(self):
		# periodically ping the successor to check if it is alive every 0.5s
		# hence works as a heart beat (timed loop)
		# run this func on a separate thread to ensure asynchronity with other functions
		failures = 0
		while self.stop == False:
			# send ping to successor (as long as tuples arent the same
			if (self.host, self.port) == self.successor and (self.host, self.port) == self.predecessor and self.predecessor == self.successor:
				time.sleep(0.5)
				continue

			# pinging to check if its still there
			try:
				self.pingingSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
				self.pingingSock.connect(self.successor)
				self.pingingSock.send(("ping, ").encode('utf-8'))
				reply = self.pingingSock.recv(1000).decode()
				signal, data = self.parseMsg(reply)
				if signal == "pingReturn":
					newSuccSucc = tuple(json.loads(data))
					if self.succsucc != newSuccSucc:
						self.succsucc = newSuccSucc
				self.pingingSock.close()
				self.pingingSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
				failures = 0
			except:
				if self.stop == True:
					break
				print("Found failure")
				failures += 1
				if failures > 2:
					print("Failure detected of ", self.successor, " from: ", (self.host, self.port))
					print("Updated successor from: ", self.successor, " to: ", self.succsucc)
					self.successor = self.succsucc
					self.pingingSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
					self.pingingSock.connect(self.succsucc)
					self.pingingSock.send(("pingKilled," + json.dumps((self.host, self.port))).encode())
					self.pingingSock.recv(1000).decode()
					for backupFile in self.backUpFiles:
						self.put(backupFile, 1)
					self.pingingSock.close()
					print("Failure handled")
					continue

			time.sleep(0.5)
		

	def parseMsg(self, msg):
		x = 0
		while True:
			if (msg[x] == ","):
				break
			x += 1
		signal = msg[0:x]
		data = msg[x+1:]
		return signal, data

	def redistributeFilesJoin(self):
		time.sleep(0.2)
		pred_hash = self.hasher(self.predecessor[0] + str(self.predecessor[1]))
		curr_hash = self.hasher(self.host + str(self.port))
		filesToRemove = []
		for file in self.files:
			hash_file = self.hasher(file)
			if hash_file < pred_hash or (pred_hash < curr_hash < hash_file):
				# send it to him
				self.put(file, 1)
				filesToRemove.append(file)
		for removing in filesToRemove:
			self.files.remove(removing)



