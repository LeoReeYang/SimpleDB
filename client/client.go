package main

import (
	"context"
	"flag"
	"log"
	"time"

	pb "github.com/LeoReeYang/SimpleDB/proto"
	"google.golang.org/grpc"
)

const (
	defaultName = "me"
)

var (
	addr = flag.String("addr", "47.113.227.34:50051", "the address to connect to")
	name = flag.String("name", defaultName, "Name to greet")
)

func main() {
	flag.Parse()
	// Set up a connection to the server.
	conn, err := grpc.Dial(*addr, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewKVClient(conn)

	// Contact the server and print out its response.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	val, err := c.GetValue(ctx, &pb.KeyRequest{Key: "1"})
	if err != nil {
		log.Fatalf("could not greet: %v", err)
	}

	log.Printf("value: %s", val.GetValue())
}
