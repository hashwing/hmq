package proto_test

import (
	"testing"
	"github.com/hashwing/hmq/proto"
)

func TestSubTopic(t *testing.T){
	topics,err:=proto.SubTopicCheckAndSpilt("/a/c/#")
	if err!=nil{
		t.Error(err)
	}
	t.Log(topics)
}