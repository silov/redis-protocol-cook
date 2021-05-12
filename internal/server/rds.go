package server

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"redis-protocol-cook/internal/resp"
)

type Server struct {
	listener  net.Listener
}

func NewRdsServer(port int) (*Server, error) {
	s := new(Server)
	var err error

	addr := fmt.Sprintf("0.0.0.0:%d", port)
	s.listener, err = net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}

	return s, nil
}

func (s *Server) onTcpConn(conn net.Conn) {
	reader := bufio.NewReader(conn)
	for {
		// ReadString 会一直阻塞直到遇到分隔符 '\n'
		// 若在遇到分隔符之前遇到异常, ReadString 会返回已收到的数据和错误信息
		msg, err := reader.ReadString('\n')
		if err != nil {
			// 通常遇到的错误是连接中断或被关闭，用io.EOF表示
			if err == io.EOF {
				// 客户端关闭连接
				log.Println("connection close")
			} else {
				log.Println(err)
			}
			return
		}
		log.Println(fmt.Sprintf("got one msg: %s", msg))
		b := []byte(fmt.Sprintf("Your msg is: %s", msg))
		// 将收到的信息发送给客户端
		conn.Write(b)
	}
}

func (s *Server) onRdsConn(conn net.Conn) error {
	defer func() {
		if err := recover(); err != nil {
			log.Printf("conn panic: %+v", err)
		}
		conn.Close()
	}()
	for {
		request, err := resp.NewRequest(conn)
		if err != nil {
			log.Println("build request err: ", err.Error())
			return err
		}

		reply := s.ServeRequest(request)
		_, err = reply.WriteTo(conn)
		return err
	}
}

func (s *Server) ServeRequest(request *resp.Request) resp.Reply {
	switch request.Command {
	case "GET":
		return s.handleGet(request)
	default:
		log.Println("ServeRequest: unsupported cmd:", request.Command)
		return resp.ErrMethodNotSupported
	}
}

func (s *Server) Serve() error {
	defer s.listener.Close()

	log.Println("server started: *:6480")

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			panic(err)
		}
		go s.onRdsConn(conn)
	}
}

func (s *Server) handleGet(r *resp.Request) resp.Reply {
	data := ""

	if r.HasArgument(0) == false {
		return resp.ErrNotEnoughArgs
	}

	if r.HasArgument(1) == true {
		return resp.ErrTooMuchArgs
	}

	key := string(r.Arguments[0])
	data = fmt.Sprintf("Your Param is: %s", key)

	return &resp.BulkReply {
		Value: []byte(data),
	}
}