package http

import (
	"client/conf"
	pb "client/confact/proto"
	"client/logs"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/valyala/fasthttp"
)

type ScanEntity struct {
	StartTs int64 `json:"start_ts"`
	EndTs   int64 `json:"end_ts"`
	Key     string `json:"key"`
	Type    pb.LogType `json:"type"`
}

type SetTypeEntity struct {
	RaftID int64 `json:"raft_id"`
	LogEntry pb.LogEntry `json:"log_entry"`
}

type DeleteLockEntity struct {
	Key   string `json:"key"`
	StartTs int64 `json:"start_ts"`
}

func RaftGet(key string,ts int64) (interface{},error){
    conf.ConfigInit()
	status, resp, err := fasthttp.Get(nil, fmt.Sprintf("http://localhost%s/transaction/get?key=%s&ts=%d", conf.TomlConf.MiddleWareHTTP, key,ts))
	if err != nil {
		logs.PrintError(-1, err.Error())
		return nil,err
	}
	if status != fasthttp.StatusOK {
		logs.PrintError(-1, "error")
		return nil,err
	}
	var response interface{}
	if err:=json.Unmarshal(resp, &response);err!=nil{
		logs.PrintError(-1,err.Error())
		return nil,err
	}
	return response,nil
}

func RaftSet(entity pb.LogEntry) (interface{},error){
	conf.ConfigInit()
	fastReq := &fasthttp.Request{}
	fastReq.SetRequestURI(fmt.Sprintf("http://localhost%s/transaction/set",  conf.TomlConf.MiddleWareHTTP))
	fastReq.Header.SetMethod("POST")
	fastReq.Header.SetContentType("application/json")
	req:=&SetTypeEntity{
		LogEntry: entity,
	}
	data, _ := json.Marshal(req)
	fastReq.SetBody(data)
	resp := &fasthttp.Response{}
	client := &fasthttp.Client{}


	if err := client.Do(fastReq, resp); err != nil {
		logs.PrintError(-1, err.Error())
		return nil,err
	}
	var response interface{}
	if err:=json.Unmarshal(resp.Body(), &response);err!=nil{
		logs.PrintError(-1,err.Error())
	}
	return response,nil
}

func RaftScan(startTs,endTs int64,key string,operationType pb.LogType) (interface{},error){
	conf.ConfigInit()

	fastReq := &fasthttp.Request{}
	fmt.Println(fmt.Sprintf("http://localhost%s/transaction/scan", conf.TomlConf.MiddleWareHTTP))
	fastReq.SetRequestURI(fmt.Sprintf("http://localhost%s/transaction/scan", conf.TomlConf.MiddleWareHTTP))
	fastReq.Header.SetMethod("POST")
	fastReq.Header.SetContentType("application/json")
	scanEntity:=&ScanEntity{
		StartTs: startTs,
		EndTs: endTs,
		Key: key,
		Type: operationType ,
	}
	data, _ := json.Marshal(scanEntity)
	fastReq.SetBody(data)
	resp := &fasthttp.Response{}
	client := &fasthttp.Client{}

	if err := client.Do(fastReq, resp); err != nil {
		logs.PrintError(-1, err.Error())
		return nil,err
	}
	var response interface{}
	if err:=json.Unmarshal(resp.Body(), &response);err!=nil{
		logs.PrintError(-1,err.Error())
	}
	if _,ok:=response.(bool);!ok{
		return false,nil
	}
	if response.(bool){
		return true,errors.New("conflict")
	}else{
		return false,nil
	}
}


