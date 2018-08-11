package client

import (
	"net"
	"sync"
	"time"
	"fmt"
	"context"

	"github.com/eclipse/paho.mqtt.golang/packets"
)

const (
	QosAtMostOnce byte = iota
	QosAtLeastOnce
	QosExactlyOnce
	QosFailure = 0x80
)

// Client a user client
type Client struct{
	conn  		net.Conn
	Connected   bool
	Info		*Info
	lock		*sync.Mutex
	ctx        context.Context
	cancelFunc context.CancelFunc
}

// Info client info
type Info struct{
	ClientID 	string
	Username 	string
	Password 	string
	WillMsg     *packets.PublishPacket
	Keepalive  	uint16
	LocalAddr	string
	RemoteAddr  string
}

// New new a client
func New(conn net.Conn)(*Client,error){
	ctx,cancelFunc:=context.WithCancel(context.Background())
	c:=&Client{
		conn:conn,
		lock:new(sync.Mutex),
		ctx:ctx,
		cancelFunc:cancelFunc,
	}
	return c,c.handleConnection()
}

func (c *Client)handleConnection()error{
	packet,err:=packets.ReadPacket(c.conn)
	if err!=nil{
		return err
	}
	if packet==nil{
		return fmt.Errorf("received nil packet")
	}
	msg, ok := packet.(*packets.ConnectPacket)
	if !ok {
		
		return fmt.Errorf("received msg that was not Connect")
	}
	connack := packets.NewControlPacket(packets.Connack).(*packets.ConnackPacket)
	connack.ReturnCode = packets.Accepted
	connack.SessionPresent = msg.CleanSession
	err = connack.Write(c.conn)
	if err != nil {
		return err
	}

	willmsg := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	if msg.WillFlag {
		willmsg.Qos = msg.WillQos
		willmsg.TopicName = msg.WillTopic
		willmsg.Retain = msg.WillRetain
		willmsg.Payload = msg.WillMessage
		willmsg.Dup = msg.Dup
	} else {
		willmsg = nil
	}
	c.Info=&Info{
		ClientID:  msg.ClientIdentifier,
		Username:  msg.Username,
		Password:  string(msg.Password),
		Keepalive: msg.Keepalive,
		WillMsg:   willmsg,
		LocalAddr: c.conn.LocalAddr().String(),
		RemoteAddr: c.conn.RemoteAddr().String(), 
	}
	c.Connected=true
	return nil
}

func (c *Client) keepAlive(ch chan int,call func(packet packets.ControlPacket,err error)) {
	defer close(ch)

	keepalive := time.Duration(c.Info.Keepalive*3/2) * time.Second
	timer := time.NewTimer(keepalive)

	for {
		select {
		case <-ch:
			timer.Reset(keepalive)
		case <-timer.C:

			call(nil,fmt.Errorf("Client exceeded timeout, disconnecting. ClientID:%s,keepalive:%v", c.Info.ClientID, c.Info.Keepalive))
			c.Connected=false
			timer.Stop()
			return
		case <-c.ctx.Done():
			return
		}
	}
}

func (c *Client)ReadLoop(call func(packet packets.ControlPacket,err error)){
	ch := make(chan int, 1000)
	for {
		select {
		case <-c.ctx.Done():
			return
		default:
			packet, err := packets.ReadPacket(c.conn)
			if err != nil {
				call(packet,err)
				return
			}
			// keepalive channel
			ch <- 1

			call(packet,nil)
		}
	}
}

func (c *Client)ProcessPublish(packet *packets.PublishPacket,onCompleteFunc func(status bool))error {
	if !c.Connected{
		return fmt.Errorf("client %s disconnect",c.Info.ClientID)
	}

	topic := packet.TopicName

	switch packet.Qos {
	case QosAtMostOnce:
		c.WriterPacket(packet)
	case QosAtLeastOnce:
		puback := packets.NewControlPacket(packets.Puback).(*packets.PubackPacket)
		puback.MessageID = packet.MessageID
		if err := c.WriterPacket(puback); err != nil {
			log.Error("send puback error, ", zap.Error(err), zap.String("ClientID", c.info.clientID))
			return
		}
		c.WriterPacket(packet)
	case QosExactlyOnce:
		return
	default:
		log.Error("publish with unknown qos", zap.String("ClientID", c.info.clientID))
		return
	}
	if packet.Retain {
		if b := c.broker; b != nil {
			err := b.rl.Insert(topic, packet)
			if err != nil {
				log.Error("Insert Retain Message error: ", zap.Error(err), zap.String("ClientID", c.info.clientID))
			}
		}
	}

}

func (c *Client) Close() {
	c.lock.Lock()
	defer c.lock.Unlock()
	if !c.Connected {
		return
	}
	c.cancelFunc()

	c.Connected=false
	//wait for message complete
	time.Sleep(1 * time.Second)
	// c.status = Disconnected

	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}
}

func (c *Client) WriterPacket(packet packets.ControlPacket) error {
	if !c.Connected {
		return nil
	}

	if packet == nil {
		return nil
	}
	if c.conn == nil {
		c.Close()
		return fmt.Errorf("connect lost ....")
	}

	c.lock.Lock()
	err := packet.Write(c.conn)
	c.lock.Unlock()
	return err
}
