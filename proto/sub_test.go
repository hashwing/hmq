package proto_test

import (
	"testing"
	"github.com/hashwing/hmq/proto"
)

func TestAdd(t *testing.T){
	st:=proto.NewSubTree()
	suber:=proto.NewSubscriber("a/b/c","1","1",0)
	err:=st.Add(suber)
	if err!=nil{
		t.Error(err)
	}
	st.Add(proto.NewSubscriber("a/b/+","2","1",0))
	st.Add(proto.NewSubscriber("a/#","3","1",0))
	st.Add(proto.NewSubscriber("a/+/c","4","1",0))
	st.Add(proto.NewSubscriber("+/+/c","5","1",0))
	st.Add(proto.NewSubscriber("+/+/d","6","1",0))
	res,err:=st.GetSubscribers("a/b/c")
	if err!=nil{
		t.Error(err)
	}
	for _,r:=range res{
		t.Log(r.ClientID)
	}
	err=st.Remove(suber)
	if err!=nil{
		t.Error(err)
	}
	res1,err:=st.GetSubscribers("a/b/c")
	if err!=nil{
		t.Error(err)
	}
	for _,r:=range res1{
		t.Log("remove",r.ClientID)
	}
}