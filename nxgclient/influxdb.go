package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"time"

	"github.com/influxdb/influxdb/cluster"
	"github.com/influxdb/influxdb/protocol"
)

const (
	KILOBYTE          = 1024
	MEGABYTE          = 1024 * KILOBYTE
	MAX_RESPONSE_SIZE = MEGABYTE * 2
)

type NxgClient struct {
	address       string
	conn          net.Conn
	lastRequestId uint32
	stopped       bool
}

func NewNxgClient(address string) *NxgClient {
	return &NxgClient{
		address:       address,
		stopped:       false,
		lastRequestId: uint32(time.Now().Unix()),
	}
}

func (self *NxgClient) Connect() error {
	conn, err := net.Dial("tcp", self.address)
	if err != nil {
		fmt.Printf("failed to connect to %s due to %s\n", self.address, err)
		return err
	}

	self.conn = conn
	fmt.Printf("connected to %s\n", self.address)
	return nil
}

func (self *NxgClient) Close() {
	if self.conn != nil {
		self.conn.Close()
	}
}

func (self *NxgClient) MakeRequest(request *protocol.Request) error {
	if request.Id == nil {
		id := atomic.AddUint32(&self.lastRequestId, uint32(1))
		request.Id = &id
	}
	data, err := request.Encode()
	if err != nil {
		return err
	}

	conn := self.conn
	if conn == nil {
		return fmt.Errorf("Failed to connect to server %s", self.address)
	}

	buff := bytes.NewBuffer(make([]byte, 0, len(data)+8))
	binary.Write(buff, binary.LittleEndian, uint32(len(data)))
	buff.Write(data)
	_, err = conn.Write(buff.Bytes())

	if err != nil {
		return err
	}

	return nil
}

func (self *NxgClient) ReadResponses(r cluster.ResponseChannel) {
	message := make([]byte, 0, MAX_RESPONSE_SIZE)
	buff := bytes.NewBuffer(message)
	for !self.stopped {
		buff.Reset()
		conn := self.conn

		var messageSizeU uint32
		var err error
		err = binary.Read(conn, binary.LittleEndian, &messageSizeU)
		if err != nil {
			fmt.Printf("Error while reading messsage size: %d\n", err)
			time.Sleep(200 * time.Millisecond)
			continue
		}
		messageSize := int64(messageSizeU)
		messageReader := io.LimitReader(conn, messageSize)
		_, err = io.Copy(buff, messageReader)
		if err != nil {
			fmt.Printf("Error while reading message: %d\n", err)
			time.Sleep(200 * time.Millisecond)
			continue
		}
		response, err := protocol.DecodeResponse(buff)
		if err != nil {
			fmt.Printf("error unmarshaling response: %s\n", err)
			time.Sleep(200 * time.Millisecond)
		} else {
			self.sendResponse(r, response)
		}
	}
}

func (self *NxgClient) sendResponse(r cluster.ResponseChannel, response *protocol.Response) {
	switch rt := response.GetType(); rt {
	case protocol.Response_END_STREAM,
		protocol.Response_HEARTBEAT,
		protocol.Response_ERROR,
		protocol.Response_QUERY:
		// do nothing
	default:
		panic(fmt.Errorf("Unknown response type: %s", rt))
	}

	r.Yield(response)
}

func main() {

	client := NewNxgClient("localhost:2202")

	client.Connect()

	responseChan := make(chan *protocol.Response, 1)
	QueryType := protocol.Request_QUERY
	WriteType := protocol.Request_WRITE
	isDbUser := true

	rcw := cluster.NewResponseChannelWrapper(responseChan)
	go client.ReadResponses(rcw)

	for {
		request := &protocol.Request{
			Type:     &QueryType,
			Database: protocol.String("dev"),
			Query:    protocol.String("select * from golang"),
			UserName: protocol.String("root"),
			IsDbUser: &isDbUser,
			Password: protocol.String("root"),
		}

		client.MakeRequest(request)
		resp := <-responseChan
		if *resp.Type == protocol.Response_ERROR {
			break
		}
		fmt.Printf("%v\n", resp)
		request = &protocol.Request{
			Type:        &WriteType,
			Database:    protocol.String("dev"),
			MultiSeries: []*protocol.Series{},
			UserName:    protocol.String("root"),
			Password:    protocol.String("root"),
		}
		for _, series := range resp.MultiSeries {
			*series.Name = "_new." + *series.Name
			for _, point := range series.Points {
				point.SequenceNumber = nil
			}
			request.MultiSeries = append(request.MultiSeries, series)
		}
		client.MakeRequest(request)
		time.Sleep(15000 * time.Millisecond)
	}

	client.Close()
}
