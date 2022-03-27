package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"

	"github.com/LeoReeYang/SimpleDB"
	pb "github.com/LeoReeYang/SimpleDB/proto"
	"google.golang.org/grpc"
)

var (
	port = flag.Int("port", 50051, "The server port")
)

type structsServer struct {
	bitcask SimpleDB.Bitcask

	pb.UnimplementedKVServer
}

func (s *structsServer) GetValue(ctx context.Context, key *pb.KeyRequest) (*pb.GetResponse, error) {
	keyStr := key.GetKey()

	fmt.Println(keyStr)
	vals, err := s.bitcask.Get(keyStr)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("values:", vals)

	return &pb.GetResponse{Value: vals}, nil
}

var key1, value = "1", "cy is god!"

func NewServer() *structsServer {
	s := &structsServer{bitcask: *SimpleDB.NewBitcask()}
	s.bitcask.Set(key1, value)
	return s
}

func main() {

	flag.Parse()
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	s := grpc.NewServer()
	pb.RegisterKVServer(s, NewServer())

	log.Printf("server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
