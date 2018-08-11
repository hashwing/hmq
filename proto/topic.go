package proto


import (
	"errors"
	"strings"
)

// import (
// 	topic "github.com/00imvj00/TopicStore"
// )

// func AddTopic(){
// 	ts:=topic.NewTopicStore()
// 	suber:=topic.Subscriber{Id:"1233",Qos:[]byte{1}}
// 	err:=ts.Add("a/+/b",suber)
// 	if err!=nil{

// 	}
// 	sb,err:=ts.GetSubscribers("a/b")
// }

func SubTopicCheckAndSpilt(topic string) ([]string, error) {
	if strings.Index(topic, "#") != -1 && strings.Index(topic, "#") != len(topic)-1 {
		return nil, errors.New("Topic format error with index of #")
	}
	re := strings.Split(topic, "/")
	for i, v := range re {
		if i != 0 && i != (len(re)-1) {
			if v == "" {
				return nil, errors.New("Topic format error with index of //")
			}
			if strings.Contains(v, "+") && v != "+" {
				return nil, errors.New("Topic format error with index of +")
			}
		} else {
			if v == "" {
				re[i] = "/"
			}
		}
	}
	return re, nil

}

func PubTopicCheckAndSpilt(topic string) ([]string, error) {
	if strings.Index(topic, "#") != -1 || strings.Index(topic, "+") != -1 {
		return nil, errors.New("Publish Topic format error with + and #")
	}
	re := strings.Split(topic, "/")
	for i, v := range re {
		if v == "" {
			if i != 0 && i != (len(re)-1) {
				return nil, errors.New("Topic format error with index of //")
			} else {
				re[i] = "/"
			}
		}

	}
	return re, nil
}