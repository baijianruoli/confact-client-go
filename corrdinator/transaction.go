package corrdinator

import (
	"client/http"
	"client/logs"

	"encoding/json"
	"errors"

	"log"
	"sync"
	"time"
)
import 	pb "client/confact/proto"

const END_TS=1e17

type Transaction struct{
	Flag bool
	StartTs int64
	Lock sync.Mutex
	Operation []*pb.Entry
	Cache  sync.Map
}

func BuildTransaction() *Transaction{
	return &Transaction{
		Operation: make([]*pb.Entry,0)}
}

func (t *Transaction) Put(key string, value interface{}) {
	data,_:=json.Marshal(value)
	entry:=&pb.Entry{
		Key: key,
		Values: &pb.Values{
			Value: data,
		},
	}
	t.Lock.Lock()
	defer t.Lock.Unlock()
    t.Cache.Store(key,value)
	t.Operation=append(t.Operation,entry)
}

func (t *Transaction) Get(key string) interface{} {
	// client缓存
	if v,ok:=t.Cache.Load(key);ok{
		return v
	}
	if _,err:=http.RaftScan(0,t.StartTs-1,key,pb.LogType_LOCK);err!=nil{
		return errors.New("conflict")
	}
	response,err:=http.RaftGet(key,t.StartTs)
	if err!=nil{
		logs.PrintError(0,err.Error())
		return nil
	}
   return response
}

func (t *Transaction) Begin() {
	t.Lock.Lock()
	defer t.Lock.Unlock()
	t.Flag = true
	t.StartTs=time.Now().UnixNano()/1e6
}

func (t *Transaction) preWrite(node, primary *pb.Entry) error {
	// 事务开始之后，不能再写入
	if _,err:=http.RaftScan(t.StartTs,END_TS,node.Key,pb.LogType_WRITE);err!=nil{
		return errors.New("conflict")
	}
	if _,err:=http.RaftScan(0,END_TS,node.Key,pb.LogType_LOCK);err!=nil{
		return errors.New("conflict")
	}

	_,err:=http.RaftSet(pb.LogEntry{
		Command: &pb.Entry{
			Key: node.Key,
			LogType: pb.LogType_DATA,
			Values: &pb.Values{
				StartTs: t.StartTs,
				Value: node.Values.Value,
			},
		},
	})

	_,err=http.RaftSet(pb.LogEntry{
		Command: &pb.Entry{
			Key: node.Key,
			LogType: pb.LogType_LOCK,
			Lock: &pb.Lock{
				StartTs: t.StartTs,
				PrimaryRow:primary.Key,
			},
		},
	})

	if err != nil {
		log.Fatal("put error")
		return errors.New("put error")
	}


	return nil
}

func (t *Transaction) write(op *pb.Entry, commitTs int64) error {
	if response,err:=http.RaftScan(t.StartTs,t.StartTs,op.Key,pb.LogType_LOCK);err!=nil&&!response.(bool){
		return errors.New("conflict")
	}
	_,err:=http.RaftSet(pb.LogEntry{
		Command: &pb.Entry{
			Key: op.Key,
			LogType: pb.LogType_WRITE,
			Write: &pb.Write{
				StartTs: t.StartTs,
				CommitTs: commitTs,
			},
		},
	})

	if err != nil {
		log.Fatal("put error")
		return errors.New("put error")
	}
	_,err=http.RaftSet(pb.LogEntry{Command: &pb.Entry{Key: op.Key,LogType:pb.LogType_DELETE_LOCK,Lock: &pb.Lock{StartTs: t.StartTs}}})
	if err != nil {
		log.Fatal("MvccDelete error")
		return errors.New("MvccDelete error")
	}
	return nil
}

func (t *Transaction) Commit() error {
	t.Lock.Lock()
	defer t.Lock.Unlock()
	// primaryRow写入
	var primary *pb.Entry
	if len(t.Operation) > 0 {
		primary = t.Operation[0]
		if err := t.preWrite(primary, primary); err != nil {
			t.Rollback()
			return err
		}
	}
	//  secondary写入
	t.Operation = t.Operation[1:]
	for _, v := range t.Operation {
		if err := t.preWrite(v, primary); err != nil {
			t.Rollback()
			return err
		}
	}
	// 开始commit
	commitTs := time.Now().Unix()
	if primary != nil {
		if err := t.write(primary, commitTs); err != nil {
			t.Rollback()
			return err
		}
	}
	// 异步执行剩下的节点
	for _, v := range t.Operation {
		go t.write(v,commitTs)
	}
	return nil
}

// 回滚节点
func (t *Transaction) Rollback() {
	for _, v := range t.Operation {
		_,_=http.RaftSet(pb.LogEntry{Command: &pb.Entry{Key: v.Key,LogType:pb.LogType_DELETE_LOCK,Lock: &pb.Lock{StartTs: t.StartTs}}})
	}
	t.Operation=t.Operation[0:0]
}
