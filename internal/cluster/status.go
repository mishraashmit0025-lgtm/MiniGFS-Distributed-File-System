package cluster

import (
	"context"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/mishraashmit0025-lgtm/MiniGFS-Distributed-File-System/client"
	pb "github.com/mishraashmit0025-lgtm/MiniGFS-Distributed-File-System/proto/gfspb"
)

// statusOf returns the live chunkservers seen by the master at addr.
func statusOf(_ *client.Client, addr string) ([]string, error) {
	cc, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	defer cc.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	st, err := pb.NewMasterClient(cc).Status(ctx, &pb.StatusRequest{})
	if err != nil {
		return nil, err
	}
	return st.LiveChunkservers, nil
}
