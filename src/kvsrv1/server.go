package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type kvEntry struct {
	value   string
	version rpc.Tversion
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	Kvs map[string]kvEntry
}

func MakeKVServer() *KVServer {
	kv := &KVServer{
		Kvs: make(map[string]kvEntry),
	}
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	entry, ok := kv.Kvs[args.Key]

	if !ok {
		reply.Err = rpc.ErrNoKey
		return
	}

	reply.Value = entry.value
	reply.Version = entry.version
	reply.Err = rpc.OK
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	entry, ok := kv.Kvs[args.Key]
	// 没有该key
	if !ok {
		// 请求版本不为0，报错
		if args.Version != 0 {
			reply.Err = rpc.ErrNoKey
			return
		}
		// 请求版本为0，新增该key
		kv.Kvs[args.Key] = kvEntry{value: args.Value, version: 1}
		reply.Err = rpc.OK
		return
	}
	// 有该key，版本不匹配
	if entry.version != args.Version {
		reply.Err = rpc.ErrVersion
		return
	}
	// 版本匹配，更新该key
	kv.Kvs[args.Key] = kvEntry{value: args.Value, version: entry.version + 1}
	reply.Err = rpc.OK
}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
