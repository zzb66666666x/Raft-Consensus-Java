all:
	rm -rf raft_build
	mkdir raft_build
	javac RaftNode.java -d raft_build
	cd raft_build && jar -cvfm raftnode.jar ../manifest.mf *
	echo "#!/usr/bin/java -jar" > raft
	cat raft_build/raftnode.jar >> raft

clean:
	rm -rf raft_build

