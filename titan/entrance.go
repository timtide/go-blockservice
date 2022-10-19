package titan

import (
	"context"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
)

const AppName = "edge"

// GetBlockFromTitan request data from titan and Convert the get data into blocks
func GetBlockFromTitan(ctx context.Context, k cid.Cid) (blocks.Block, error) {
	if !k.Defined() {
		return nil, ipld.ErrNotFound{Cid: k}
	}

	// create titan client object
	client, err := NewClientTitan(ctx)
	if err != nil {
		return nil, err
	}

	// request data by cid
	data, err := client.GetDataFromEdgeNode(k)
	if err != nil {
		return nil, err
	}

	return blocks.NewBlockWithCid(data, k)
}
