package titan

import (
	"bytes"
	"fmt"
	"github.com/ipfs/go-cid"
	"io"
	"net/http"
	"time"
)

// GetBlockByHttp connect Titan net by http get method
func GetBlockByHttp(host, token string, cid cid.Cid) ([]byte, error) {
	// set http request timed out five second
	client := &http.Client{Timeout: 5 * time.Second}
	url := fmt.Sprintf("%s%s%s", host, "?cid=", cid.String())
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
