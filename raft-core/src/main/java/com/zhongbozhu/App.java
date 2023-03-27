package com.zhongbozhu;

import java.util.ArrayList;
import java.util.concurrent.Semaphore;
import java.lang.Math;
import java.io.*;

class RPCRequestVotes {
    public int term;
    public int candidateId;
    public int lastLogIndex;
    public int lastLogTerm;

    public RPCRequestVotes(String[] msg){
        term = Integer.parseInt(msg[3]);
        candidateId = Integer.parseInt(msg[4]);
        lastLogIndex = Integer.parseInt(msg[5]);
        lastLogTerm = Integer.parseInt(msg[6]);
    }         
}

class RPCReplyVote {
    public int success;
    public int term;

    public RPCReplyVote(String[] msg){
        success = Integer.parseInt(msg[3]);
        term  = Integer.parseInt(msg[4]);
    }
}

class RPCAppendEntries {
    public int term;
    public int leaderId;
    public int prevLogIndex;
    public int prevLogTerm;
    public int leaderCommit;
    public int newEntryTerm;
    public String entryContent;

    RPCAppendEntries(String[] msg){
        term = Integer.parseInt(msg[3]);
        leaderId = Integer.parseInt(msg[4]);
        prevLogIndex = Integer.parseInt(msg[5]);
        prevLogTerm = Integer.parseInt(msg[6]);
        leaderCommit = Integer.parseInt(msg[7]);
        newEntryTerm = Integer.parseInt(msg[8]);
        if (msg.length>=10){
            entryContent = msg[9];
        }else{
            entryContent = "";
        }
    }
}

class RPCReply {
    public int success;
    public int term;
    public int matchedLogIndex;

    public RPCReply(String[] msg){
        success = Integer.parseInt(msg[3]); 
        term  = Integer.parseInt(msg[4]);
        matchedLogIndex = Integer.parseInt(msg[5]); 
    }
}

class LogEntry{
    public int term;
    public String entryContent;

    public LogEntry(int p1, String p2){
        term = p1;
        entryContent = p2;
    }
}

class Server{
    static Semaphore sem = new Semaphore(1);
    static int pid;
    static int processNumber;
    static String curState = "FOLLOWER";
    static long curTime;
    static long timeout;
    static int term = 1;
    static int replyCount = 1;
    static int votedFor = -1;
    static int leaderId = -1;
    static int lastLogIndex;
    static int lastLogTerm;
    static ArrayList<LogEntry> logs = new ArrayList<LogEntry>();
    static int nextIndexList[];
    static int matchedLogIndexList[];
    static int lastCommitLogIndex = 0;

    static long TimeoutRange = 1500000000;
    static long Heartbeat = 30000000;

    public static void setLeader(int leader_pid){
        try{
            sem.acquire();
            leaderId = leader_pid;
            sem.release();
        }catch(InterruptedException e){
            System.exit(0);
        }
    }

    public static int getLeader(){
        int ret = 0;
        try{
            sem.acquire();
            ret = leaderId;
            sem.release();
        }catch(InterruptedException e){
            System.exit(0);
        }
        return ret;
    }

    public static int getTerm(){
        int ret = 0;
        try{
            sem.acquire();
            ret = term;
            sem.release();
        }catch(InterruptedException e){
            System.exit(0);
        }
        return ret;
    }

    public static int getLastCommit(){
        int ret = 0;
        try{
            sem.acquire();
            ret = lastCommitLogIndex;
            sem.release();
        }catch(InterruptedException e){
            System.exit(0);
        }
        return ret;
    }

    public static void refreshState(int nextTerm, String nextState){
        try{
            sem.acquire();
            // update time
            curTime = System.nanoTime();
            // change state if new
            if (nextState.equals(curState) == false){
                curState = nextState;
                System.out.println("STATE state=\"" + curState + "\"");
            }
            // change term
            if (nextTerm != term){
                // reset leader and votedFor
                if (leaderId != -1){
                    votedFor = -1;
                    leaderId = -1;
                    System.out.println("STATE leader=null");
                }
                term = nextTerm;
                System.out.println("STATE term=" + String.valueOf(nextTerm));
                // reset timeout for new term
                timeout = TimeoutRange + (long)(Math.random() * TimeoutRange);
                // reset reply count of leader electino
                replyCount = 1;
                // reset log status
                for (int i=0; i<Server.processNumber; i++){
                    Server.nextIndexList[i] = Server.lastLogIndex+1;
                    Server.matchedLogIndexList[0] = 0;
                }
            }
            sem.release();
        }catch(InterruptedException e){
            System.exit(0);
        }
    }
}

class ReceiveMessage extends Thread{
    @Override
    public void run(){
        // System.out.println("starting to receive message");
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        while (true){
            try{
                String msgText = br.readLine();
                if (msgText == null){
                    br.close();
                    System.exit(0);
                }
                // System.err.println(String.valueOf(Server.pid) + ">" + msgText);
                String msg[] = msgText.split(" ", -1);
                if (msg[0].equals("LOG")){
                    Server.sem.acquire();
                    if (Server.curState.equals("LEADER") == false){
                        Server.sem.release();
                        continue;
                    }
                    LogEntry le = new LogEntry(Server.term, msg[1]);
                    Server.logs.add(le);
                    Server.lastLogTerm = Server.term;
                    Server.lastLogIndex ++;
                    String logReport = String.format("STATE log[%d]=[%d,\"%s\"]", Server.lastLogIndex, Server.term, le.entryContent);
                    Server.sem.release();
                    System.out.println(logReport);
                }
                else if (msg[2].equals("RequestVote")){
                    RPCRequestVotes rv = new RPCRequestVotes(msg);
                    Server.sem.acquire();
                    if ((rv.term > Server.term) || // higher term leader?
                        (Server.term == rv.term && (Server.lastLogIndex <= rv.lastLogIndex) && (Server.lastLogTerm <= rv.lastLogTerm) && // same term, but newer log?
                        (Server.votedFor==-1 || Server.votedFor == rv.candidateId))){ // have I voted for someone else in this term?
                        Server.sem.release();
                        Server.refreshState(rv.term, "FOLLOWER");
                        Server.sem.acquire();
                        Server.votedFor = rv.candidateId;
                        Server.sem.release();
                        String s1 = String.format("SEND %d ReplyVote 1 %d", rv.candidateId, rv.term);
                        System.out.println(s1);
                    }
                    Server.sem.release();
                }
                else if (msg[2].equals("ReplyVote")){
                    RPCReplyVote rep = new RPCReplyVote(msg);
                    Server.sem.acquire();
                    if (rep.success == 1 && Server.curState.equals("CANDIDATE") && Server.term == rep.term){
                        Server.replyCount ++;
                        Server.sem.release();
                    }else if (rep.term > Server.term){
                        Server.sem.release();
                        Server.refreshState(rep.term, "FOLLOWER");
                    }
                    Server.sem.release();
                }
                else if (msg[2].equals("AppendEntries")){
                    RPCAppendEntries appendEntry = new RPCAppendEntries(msg);
                    // heartbeat
                    if (appendEntry.term >= Server.term){
                        int prevLeader = Server.getLeader();
                        Server.refreshState(appendEntry.term, "FOLLOWER");
                        if (prevLeader != appendEntry.leaderId || appendEntry.term > Server.getTerm()){
                            Server.setLeader(appendEntry.leaderId);
                            System.out.println("STATE leader=\"" + appendEntry.leaderId + "\"");
                        }
                        // ready to process real log append entry
                        if (msg.length >= 10){
                            // check consistency
                            Server.sem.acquire();
                            if (Server.lastLogIndex >= appendEntry.prevLogIndex && Server.logs.get(appendEntry.prevLogIndex).term == appendEntry.prevLogTerm ){
                                // consistency checked
                                Server.lastLogIndex = appendEntry.prevLogIndex + 1;
                                // Server.lastLogTerm = appendEntry.newEntryTerm;
                                LogEntry le = new LogEntry(appendEntry.newEntryTerm, appendEntry.entryContent);
                                if (Server.logs.size()-1 >= Server.lastLogIndex){
                                    Server.logs.set(Server.lastLogIndex, le);
                                }else{
                                    Server.logs.add(le);
                                }
                                // organize reply
                                String replyString = String.format("SEND %d Reply 1 %d %d", appendEntry.leaderId, Server.term, Server.lastLogIndex);
                                Server.sem.release();
                                System.out.println(replyString);
                                String stateReport = String.format("STATE log[%d]=[%d,\"%s\"]", appendEntry.prevLogIndex+1, appendEntry.term, appendEntry.entryContent);
                                System.out.println(stateReport);
                            }else{
                                String replyString = String.format("SEND %d Reply 0 %d %d", appendEntry.leaderId, Server.term, Server.lastLogIndex);
                                Server.sem.release();
                                System.out.println(replyString);
                            }
                        }
                        // update Server.lastCommitLogIndex
                        Server.sem.acquire();
                        if (appendEntry.leaderCommit > Server.lastCommitLogIndex){
                            Server.lastCommitLogIndex = Math.min(appendEntry.leaderCommit, Math.min(Server.lastLogIndex, appendEntry.prevLogIndex+1));
                            String commitReport = String.format("STATE commitIndex=%d", Server.lastCommitLogIndex);
                            System.out.println(commitReport);
                        }
                        Server.sem.release();
                    }
                }
                else if (msg[2].equals("Reply")){
                    int nodeName = Integer.parseInt(msg[1]);
                    RPCReply rep = new RPCReply(msg);
                    Server.sem.acquire();
                    if (rep.success == 1 && rep.term <= Server.term && Server.curState.equals("LEADER")){
                        // get a success reply, move commitIndex, update matchIndex
                        Server.matchedLogIndexList[nodeName] = Math.max(rep.matchedLogIndex, Server.matchedLogIndexList[nodeName]);
                        if (rep.matchedLogIndex == Server.nextIndexList[nodeName] && Server.nextIndexList[nodeName]-1 <= Server.lastLogIndex){
                            Server.nextIndexList[nodeName]++;
                        }
                        // check if the matched log index can move the commit index by +1
                        int counter = 0;
                        for (int i=0; i<Server.processNumber; i++){
                            if (Server.matchedLogIndexList[i] > Server.lastCommitLogIndex){
                                counter++;
                            }
                        }
                        if (counter >= Server.processNumber/2 + 1){
                            Server.lastCommitLogIndex++;
                            System.out.println("STATE commitIndex=" + String.valueOf(Server.lastCommitLogIndex));
                            // System.err.println(String.valueOf(Server.pid)+">STATE commitIndex=" + String.valueOf(Server.lastCommitLogIndex));
                        }
                        Server.sem.release();
                    }else if (rep.term > Server.term){
                        Server.sem.release();
                        Server.refreshState(rep.term, "FOLLOWER");
                    }else if (rep.success != 1 && Server.nextIndexList[nodeName]>1){
                        Server.nextIndexList[nodeName]--;
                        Server.sem.release();
                    }
                }
            }catch (IOException e){
                System.exit(0);
            }catch (InterruptedException e){
                System.exit(0);
            }
        }
    }
}

public class App{
    private static void enterFollower(){
        while (true){
            try{
                long timeNano = System.nanoTime();
                Server.sem.acquire();
                long timeout = Server.timeout;
                long curTime = Server.curTime;
                String curState = Server.curState;
                Server.sem.release();
                if ((curState.equals("FOLLOWER") == false) || (timeNano - curTime > timeout)){
                    if (timeNano - curTime > timeout){
                        Server.refreshState(Server.term+1, "CANDIDATE");
                    }
                    return;
                }
                // sleep for a short time and see again
                Thread.sleep(1);
            }catch(InterruptedException e){
                System.exit(0);
            }
        }
    }

    private static void enterLeader(){
        Server.setLeader(Server.pid);
        System.out.println("STATE leader=\"" + String.valueOf(Server.pid) + "\"");
        // start giving heartbeat
        long HB = Server.Heartbeat;
        long lastHB = System.nanoTime() - HB;
        while (true){
            if (Server.curState.equals("LEADER")==false){
                return;
            }
            long timeNano = System.nanoTime();
            if (timeNano - lastHB >= HB){
                lastHB = timeNano;
                for (int i=0; i<Server.processNumber; i++){
                    if (i != Server.pid){
                        try{
                            Server.sem.acquire();
                            int term  = Server.term;
                            int leaderId = Server.pid;
                            int leaderCommit = Server.lastCommitLogIndex;
                            int prevLogIndex = Server.nextIndexList[i]-1;
                            if (prevLogIndex < 0){
                                prevLogIndex = 0;
                            }
                            int prevLogTerm;
                            int newEntryTerm;
                            String newEntryContent;
                            if (Server.lastLogIndex >= Server.nextIndexList[i]){
                                prevLogTerm = Server.logs.get(Server.nextIndexList[i]-1).term;
                                newEntryTerm = Server.logs.get(Server.nextIndexList[i]).term;
                                newEntryContent = Server.logs.get(Server.nextIndexList[i]).entryContent;
                            }else{
                                // dummy place holder in RPC
                                prevLogTerm = 1;  
                                newEntryTerm = 1; 
                                newEntryContent = "";
                            }
                            Server.sem.release();
                            String s1 = String.format("SEND %d AppendEntries %d %d %d %d %d %d ", i,
                                                    term, leaderId, prevLogIndex, prevLogTerm, leaderCommit, newEntryTerm);
                            String s2 = s1 + newEntryContent;
                            System.out.println(s2);
                        }catch(InterruptedException e){
                            System.exit(0);
                        }
                    }
                }
            }
            // sleep for a short time and see again
            try{
                Thread.sleep(1);
            }catch(InterruptedException e){
                System.exit(0);
            }
        }
    }

    private static void enterCandidate(){
        // request vote
        for (int i=0; i<Server.processNumber; i++){
            if (i != Server.pid){
                try{
                    Server.sem.acquire();
                    int term = Server.term;
                    int lastLogIndex = Server.lastLogIndex;
                    int lastLogTerm = Server.lastLogTerm;
                    Server.sem.release();
                    System.out.println("SEND " + String.valueOf(i) + " RequestVote "+ String.valueOf(term) + " "
                                    + String.valueOf(Server.pid) + " " + String.valueOf(lastLogIndex + " "
                                    + String.valueOf(lastLogTerm)));
                }catch(InterruptedException e){
                    System.exit(0);
                }
            }
        }
        // wait and collect reply
        while (true){
            long timeNano = System.nanoTime();
            long curTime = Server.curTime;
            long timeout = Server.timeout;
            String curState = Server.curState;
            if ((curState.equals("CANDIDATE")==false) || (timeNano - curTime > timeout)){
                if (timeNano - curTime > timeout){
                    // do state transition or restart election
                    Server.refreshState(Server.term+1, Server.curState);
                }
                return;
            }else if (Server.replyCount >= 1+Math.floor(Server.processNumber/2.0)){
                // get enough votes
                Server.refreshState(Server.term, "LEADER");
                return;
            }
            // sleep for a short time and see again
            try{
                Thread.sleep(1);
            }catch(InterruptedException e){
                System.exit(0);
            }
        }
    }

    public static void main(String[] args) throws InterruptedException{
        Server.pid = Integer.parseInt(args[0]);
        int processNumber = Integer.parseInt(args[1]);
        Server.processNumber = processNumber;
        // time setup
        Server.curTime = System.nanoTime();
        Server.timeout = Server.TimeoutRange + (long)(Math.random() * Server.TimeoutRange);
        // put dummy log inside
        LogEntry dummyentry = new LogEntry(1, "");
        Server.logs.add(dummyentry);
        Server.lastLogIndex = 0;
        Server.lastLogTerm = 1;
        // init the index arrays to record status of the whole distributed system
        Server.nextIndexList = new int[processNumber];
        Server.matchedLogIndexList = new int[processNumber];
        for (int i=0; i<processNumber; i++){
            Server.nextIndexList[i] = 1;
            Server.matchedLogIndexList[i] = 0;
        }

        // report initial status
        System.out.println("STATE state=\"" + Server.curState + "\"");
        System.out.println("STATE leader=null");
        System.out.println("STATE term=" + String.valueOf(Server.term));

        // start receiver thread
        ReceiveMessage recv_thread = new ReceiveMessage();
        recv_thread.start();

        // do the job of curState
        while (true){
            if (Server.curState.equals("FOLLOWER")){
                enterFollower();
            }else if (Server.curState.equals("LEADER")){
                enterLeader();
            }else if (Server.curState.equals("CANDIDATE")){
                enterCandidate();
            }
        }
    }
}
