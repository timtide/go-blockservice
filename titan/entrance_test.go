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
	urls := []string{
		"/ip4/192.168.0.45/tcp/4567",
		"/ip4/192.168.0.45/tcp/3456",
		"/ip4/192.168.0.43/tcp/3456",
	}
	ctx := context.WithValue(context.Background(), "TitanIps", urls)
	block, err := GetBlockFromTitan(ctx, decodeCid)
	if err != nil {
		t.Error(err)
		return
	}
	t.Log(block.Cid())
	t.Log(block.String())
	t.Log(block.Loggable())
	t.Log(string(block.RawData()))
}
