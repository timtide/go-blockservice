package blockservice

import (
	"bytes"
	"fmt"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	"io"
	"net/http"
	"time"
)

var token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE2NjUyMDExNDB9.w_rmGH-5tX3ICIZtYda39QMXzddmFro8BgdqBEGM-90"
var AppName = "edge"

// GetBlockFromTitan Convert the get data into blocks
func GetBlockFromTitan(k cid.Cid) (blocks.Block, error) {
	logger.Infof("get block from titan By cid : %s", k.String())
	if !k.Defined() {
		logger.Error("undefined cid in block store")
		return nil, ipld.ErrNotFound{Cid: k}
	}
	data, err := GetBlockByHttp(k)
	if err != nil {
		return nil, err
	}

	return blocks.NewBlockWithCid(data, k)
}

// GetBlockByHttp connect Titan net by http get method
func GetBlockByHttp(cid cid.Cid) ([]byte, error) {
	// set http request timed out five second
	client := &http.Client{Timeout: 5 * time.Second}
	url := fmt.Sprintf("%s%s", "http://192.168.0.136:3000/block/get?cid=", cid.String())
	request, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	// set request header, eg: token
	request.Header.Set("Token", token)
	request.Header.Set("App-Name", AppName)

	// request do
	resp, err := client.Do(request)
	if err != nil {
		return nil, err
	}

	// Judge the return status
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("%s", resp.Status)
	}

	defer resp.Body.Close()

	var buffer [512]byte
	result := bytes.NewBuffer(nil)

	for {
		n, err := resp.Body.Read(buffer[0:])
		result.Write(buffer[0:n])
		if err != nil && err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
	}

	return result.Bytes(), nil
}
