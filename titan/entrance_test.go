package titan

import (
	"context"
	"github.com/ipfs/go-cid"
	"testing"
)

func TestGetBlockFromTitan(t *testing.T) {
	decodeCid, err := cid.Decode("bafkreih6ouktjwl5eiiojb2bwqmdbx4t2tx2lwtlk3fvvqwnfb532p5ywy")
	if err != nil {
		t.Error(err)
		return
	}
	block, err := GetBlockFromTitan(context.TODO(), decodeCid)
	if err != nil {
		t.Error(err)
		return
	}
	t.Log(block.Cid())
	t.Log(block.String())
	t.Log(block.Loggable())
	t.Log(string(block.RawData()))
}
