package nxgpb

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"sync"
	"time"

	"github.com/influxdb/influxdb/api"
	"github.com/influxdb/influxdb/cluster"
	"github.com/influxdb/influxdb/common"
	"github.com/influxdb/influxdb/protocol"

	log "code.google.com/p/log4go"
)

const KILOBYTE = 1024
const MEGABYTE = 1024 * KILOBYTE
const MaxRequestSize = MEGABYTE * 2

// NxgServer serves as private Protobuf API server
type NxgServer struct {
	listener          net.Listener
	port              string
	coordinator       api.Coordinator
	clusterConfig     *cluster.ClusterConfiguration
	connectionMapLock sync.Mutex
	connectionMap     map[net.Conn]bool
}

type NxgWriter struct {
	conn      net.Conn
	memSeries map[string]*protocol.Series
}

func (self *NxgWriter) Yield(series *protocol.Series) (bool, error) {
	log.Info(series)
	err := self.yield(series)
	if err != nil {
		return false, err
	}
	return true, nil
}

func (self *NxgWriter) Name() string {
	return "NxgWriter"
}

func (self *NxgWriter) Close() error {
	return nil
}

func (self *NxgWriter) yield(series *protocol.Series) error {
	oldSeries := self.memSeries[*series.Name]
	if oldSeries == nil {
		self.memSeries[*series.Name] = series
		return nil
	}

	self.memSeries[series.GetName()] = common.MergeSeries(self.memSeries[series.GetName()], series)
	return nil
}

func (self *NxgWriter) done(id *uint32) error {
	response := &protocol.Response{
		Type:        protocol.Response_END_STREAM.Enum(),
		MultiSeries: []*protocol.Series{},
		RequestId:   id,
	}
	for _, series := range self.memSeries {
		response.MultiSeries = append(response.MultiSeries, series)
	}
	data, err := response.Encode()
	if err != nil {
		return err
	}

	buff := bytes.NewBuffer(make([]byte, 0, len(data)+4))
	err = binary.Write(buff, binary.LittleEndian, uint32(len(data)))

	if err != nil {
		return err
	}

	_, err = self.conn.Write(append(buff.Bytes(), data...))
	return err
}

// NewNxgServer creates a new instance of NxgServer
func NewNxgServer(port string, clusterConfig *cluster.ClusterConfiguration, coord api.Coordinator) *NxgServer {
	server := &NxgServer{
		port:          port,
		coordinator:   coord,
		connectionMap: make(map[net.Conn]bool),
		clusterConfig: clusterConfig,
	}
	return server
}

// Close cleans up all connections and shuts down the server
func (itself *NxgServer) Close() {
	itself.listener.Close()
	itself.connectionMapLock.Lock()
	defer itself.connectionMapLock.Unlock()
	for conn := range itself.connectionMap {
		conn.Close()
	}

	// loop while the port is still accepting connections
	for {
		_, port, _ := net.SplitHostPort(itself.port)
		conn, err := net.Dial("tcp", "localhost:"+port)
		if err != nil {
			log.Error("Received error %s, assuming connection is closed.", err)
			break
		}
		conn.Close()

		log.Info("Waiting while the server port is closing")
		time.Sleep(1 * time.Second)
	}
}

// ListenAndServe listens on server's port for requests
func (itself *NxgServer) ListenAndServe() {
	_listener, err := net.Listen("tcp", "localhost:"+itself.port)
	if err != nil {
		panic(err)
	}

	itself.listener = _listener
	log.Info("NxgServer listening on %s", itself.port)
	for {
		conn, err := _listener.Accept()
		if err != nil {
			log.Error("Error with TCP connection. Assuming server is closing: %s", err)
			break
		}
		itself.connectionMapLock.Lock()
		itself.connectionMap[conn] = true
		itself.connectionMapLock.Unlock()
		go itself.HandleConnection(conn)
	}
}

// HandleConnection deals with incoming connections
func (itself *NxgServer) HandleConnection(conn net.Conn) {
	log.Info("NxgServer accepted client: %s", conn.RemoteAddr().String())

	requestPayload := make([]byte, 0, MaxRequestSize)
	buf := bytes.NewBuffer(requestPayload)

	var payloadSizeU uint32
	for {
		err := binary.Read(conn, binary.LittleEndian, &payloadSizeU)
		if err != nil {
			log.Error("Error reading from connection (%s): %s", conn.RemoteAddr().String(), err)
			itself.connectionMapLock.Lock()
			delete(itself.connectionMap, conn)
			itself.connectionMapLock.Unlock()
			conn.Close()
			return
		}

		payloadSize := int64(payloadSizeU)

		err = itself.HandleRequest(conn, payloadSize, buf)
		if err != nil {
			log.Error("Error, closing connection: %s", err)
			itself.connectionMapLock.Lock()
			delete(itself.connectionMap, conn)
			itself.connectionMapLock.Unlock()
			conn.Close()
			return
		}
		buf.Reset()
	}
}

// HandleRequest passes request to requestHandler
func (itself *NxgServer) HandleRequest(conn net.Conn, payloadSize int64, buf *bytes.Buffer) error {
	reader := io.LimitReader(conn, payloadSize)
	_, err := io.Copy(buf, reader)
	if err != nil {
		return err
	}
	request, err := protocol.DecodeRequest(buf)
	if err != nil {
		return err
	}

	log.Info("Received %s", request)

	user, err := itself.clusterConfig.AuthenticateClusterAdmin(*request.UserName, *request.Password)
	if err != nil {
		return itself.SendErrorResponse(conn, request.Id, "failed to authenticate user")
	}

	if *request.Type == protocol.Request_QUERY {
		nxgWriter := &NxgWriter{
			memSeries: map[string]*protocol.Series{},
			conn:      conn,
		}

		err = itself.coordinator.RunQuery(user, *request.Database, *request.Query, nxgWriter)

		if err != nil {
			return itself.SendErrorResponse(conn, request.Id, "failed to query")
		}

		nxgWriter.done(request.Id)
	}

	if *request.Type == protocol.Request_WRITE {
		err = itself.coordinator.WriteSeriesData(user, *request.Database, request.MultiSeries)
		if err != nil {
			return itself.SendErrorResponse(conn, request.Id, "failed to write")
		}
	}

	return nil
}

// SendErrorResponse returns an error message to client
func (itself *NxgServer) SendErrorResponse(conn net.Conn, id *uint32, message string) error {
	response := &protocol.Response{
		RequestId:    id,
		Type:         protocol.Response_ERROR.Enum(),
		ErrorMessage: protocol.String(message),
	}
	data, err := response.Encode()
	if err != nil {
		return err
	}

	buff := bytes.NewBuffer(make([]byte, 0, len(data)+4))
	err = binary.Write(buff, binary.LittleEndian, uint32(len(data)))

	if err != nil {
		return err
	}

	_, err = conn.Write(append(buff.Bytes(), data...))
	return err
}
