package blockservice

import (
	"github.com/ipfs/go-cid"
	"testing"
)

func TestGet(t *testing.T) {
	decodeCid, err := cid.Decode("QmQQ4oXiGbaii8BmqLJkqBpgSh9xvW4t5URCTLJtH6fnhb")
	if err != nil {
		t.Error(err)
		return
	}
	data, err := GetBlockByHttp(decodeCid)
	if err != nil {
		t.Error(err)
		return
	}
	t.Log(string(data))
	return
}

func TestGetBlockFromTitan(t *testing.T) {
	decodeCid, err := cid.Decode("QmTp2hEo8eXRp6wg7jXv1BLCMh5a4F3B7buAUZNZUu772j")
	if err != nil {
		t.Error(err)
		return
	}
	block, err := GetBlockFromTitan(decodeCid)
	if err != nil {
		t.Error(err)
		return
	}
	t.Log(block.Cid())
	t.Log(block.String())
	t.Log(block.Loggable())
	t.Log(string(block.RawData()))
}
