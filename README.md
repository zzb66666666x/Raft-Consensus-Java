# Raft Consensus Implemented by Java

Raft consensus protocol implemented using java. The test framework is based on python3 asyncio which can simulate various test cases in real distributed systems, like node failure and network partition. 

#### Requirements

- Java environment correctly pointed by environment variable `JAVA_HOME`.
- Maven 3.x.x should be installed.
- python3 (expecting python3.10, but python3.8 is also working fine).

#### Build

```
cd /path/of/this/repo
make
```

#### Test

Read the `raft-test-framework/README.md` to know about the test cases simulated. You can use the `./run-test-case.sh` to select test cases to run (coming soon). You can also do the following to run the raft log replication test with network partition happening over a 5-node cluster. The rest of the tests follow the same command pattern. 

```
cd raft-test-framework
python3 raft_partition_test.py 5
```