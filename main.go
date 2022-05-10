package main

import (
	"client/corrdinator"
	"client/logs"
)

func main() {
	t:=corrdinator.BuildTransaction()
	t.Begin()
    t.Put("101",77)
	if err:=t.Commit();err!=nil{
		logs.PrintError(-1,err.Error())
	}
}
