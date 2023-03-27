VERSION=1.0

ifndef JAVA_HOME
    $(error JAVA_HOME is not set)
endif

all:
	echo $(JAVA_HOME)
	cd raft-core && mvn package -DskipTests && cp target/raft-$(VERSION).jar ../raft-test-framework
	cd raft-test-framework && echo "#!$(JAVA_HOME)/bin/java -jar" > raft && cat ./raft-$(VERSION).jar >> raft && chmod +x ./raft

clean:
	cd raft-core && mvn clean
	cd raft-test-framework && rm raft && rm *.jar