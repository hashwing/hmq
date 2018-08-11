package proto


import (
	"sync"
	"errors"
)

 // Subscriber a subscriber
 type Subscriber struct {
	Topic   		string
	ClientID 		string
	NodeID 			string
	Qos				int
 }

 // NewSubscriber new Subscriber
 func NewSubscriber(topic,clientID,nodeID string,qos int)*Subscriber{
	 return &Subscriber{
		Topic: topic,
		ClientID:clientID,
		NodeID:nodeID,
		Qos:qos,
	 }
 }

// SubTree a suber tree
 type SubTree struct {
	lock *sync.RWMutex
	root *level
 }


type level struct {
	nodes map[string]*node
}

func newLevel()*level{
	return &level{
		nodes:make(map[string]*node),
	}
}

 type node struct {
	 children *level
	 subList  []*Subscriber	
 }

 func newNode()*node{
	 return &node{
		subList:make([]*Subscriber,0),
	 }
 }

 // NewSubTree create a SubeTree
 func NewSubTree()*SubTree{
	 return &SubTree{
		lock:new(sync.RWMutex),
		root:newLevel(),
	 }
 }

 // Add add a topic into SubTree
 func (st *SubTree)Add(suber *Subscriber)error{
	 topicArray,err:=SubTopicCheckAndSpilt(suber.Topic)
	 if err!=nil{
		 return err
	 }
	 st.lock.Lock()
	 defer st.lock.Unlock()
	 currentLevel :=st.root
	 var currentNode *node 
	 for _,topicItem := range topicArray{
		currentNode = currentLevel.nodes[topicItem]
		if currentNode == nil{
			currentNode = newNode()
			currentLevel.nodes[topicItem]=currentNode
		}
		if currentNode.children == nil{
			currentNode.children=newLevel()
		}
		currentLevel=currentNode.children
	 }
	 for _,s:=range currentNode.subList{
		if s.ClientID==suber.ClientID{
			return nil
		}
	 }
	 currentNode.subList=append(currentNode.subList,suber)
	 return nil
 }

 // Remove remove a suber from tree
 func (st *SubTree)Remove(suber *Subscriber)error{
	topicArray,err:=SubTopicCheckAndSpilt(suber.Topic)
	if err!=nil{
		return err
	}
	st.lock.Lock()
	defer st.lock.Unlock() 
	l := st.root
	var n *node

	for _, t := range topicArray {
		if l == nil {
			return errors.New("No Matches subscription Found")
		}
		n = l.nodes[t]
		if n != nil {
			l = n.children
		} else {
			l = nil
		}
	}
	if n==nil{
		return errors.New("No Matches subscription Found")
	}
	for i,s:=range n.subList{
		if s.ClientID==suber.ClientID{
			n.subList=append(n.subList[:i],n.subList[i+1:]...)
		}
	}
	return nil
 }

 // GetSubscribers get Subscribers by topic
 func (st *SubTree)GetSubscribers(topic string)([]*Subscriber,error){
	topicArray,err := PubTopicCheckAndSpilt(topic)
	if err!=nil{
		return nil,err
	}
	st.lock.Lock()
	defer st.lock.Unlock()
	subers:=make([]*Subscriber,0)
	currentLevel :=st.root
	if len(topicArray)>0{
		if topicArray[0] == "/" {
			if _, exist := currentLevel.nodes["#"]; exist {
				subers = append(subers,currentLevel.nodes["#"].subList...)
			}
			if _, exist := currentLevel.nodes["+"]; exist {
				matchLevel(currentLevel.nodes["/"].children, topicArray[1:], &subers)
			}
			if _, exist := currentLevel.nodes["/"]; exist {
				matchLevel(currentLevel.nodes["/"].children, topicArray[1:], &subers)
			}
		} else {
			matchLevel(st.root, topicArray, &subers)
		}
	}
	return subers,nil
 }

 func matchLevel(l *level, toks []string, subers *[]*Subscriber) {
	var swc, n *node
	exist := false
	for i, t := range toks {
		if l == nil {
			return
		}

		if _, exist = l.nodes["#"]; exist {
			*subers = append(*subers,l.nodes["#"].subList...)
		}
		if t != "/" {
			if swc, exist = l.nodes["+"]; exist {
				matchLevel(l.nodes["+"].children, toks[i+1:], subers)
			}
		} else {
			if _, exist = l.nodes["+"]; exist {
				*subers = append(*subers,l.nodes["+"].subList...)
			}
		}

		n = l.nodes[t]
		if n != nil {
			l = n.children
		} else {
			l = nil
		}
	}
	if n != nil {
		*subers = append(*subers,n.subList...)
	}
	if swc != nil { 
		*subers = append(*subers,swc.subList...)
	}
}
