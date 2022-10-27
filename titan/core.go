package titan

import (
	"context"
	"errors"
	"fmt"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/api/client"
	"time"
)

var logger = logging.Logger("blockservice/titan")

type ClientOfTitan struct {
	ctx           context.Context
	SchedulerURLs []string
}

func NewClientTitan(ctx context.Context) (*ClientOfTitan, error) {
	value := ctx.Value("TitanIps")

	ct := &ClientOfTitan{
		ctx: ctx,
	}
	if multiAddrStrings, ok := value.([]string); ok {
		urls, err := transformationMultiAddrStringsToUrl(multiAddrStrings)
		if err != nil {
			return nil, err
		}
		ct.SchedulerURLs = urls
	} else {
		return nil, fmt.Errorf("%s", "multi addresses assertion failure")
	}
	return ct, nil
}

// get edge url and token from titan schedule service
func (c *ClientOfTitan) getDownloadInfoFromScheduleService(cid cid.Cid) (*api.DownloadInfo, error) {
	ch := make(chan *api.DownloadInfo)
	// defer close(ch)
	ctx, cancel := context.WithCancel(c.ctx)
	defer cancel()
	for _, v := range c.SchedulerURLs {
		value := v
		go func(cx context.Context, url string) {
			apiScheduler, closer, err := client.NewScheduler(c.ctx, url, nil)
			if err != nil {
				logger.Error(err.Error())
				return
			}
			defer closer()
			downloadInfo, err := apiScheduler.GetDownloadInfoWithBlock(c.ctx, cid.String(), "120.24.37.24")
			if err != nil {
				logger.Error(err.Error())
				return
			}
			select {
			case <-cx.Done():
				return
			case ch <- &downloadInfo:
				return
			}
		}(ctx, value)
	}
	select {
	case df := <-ch:
		return df, nil
	case <-time.Tick(5 * time.Second):
		return nil, fmt.Errorf("%s", "get download info from titan schedule service time out")
	}
}

func (c *ClientOfTitan) GetDataFromEdgeNode(cid cid.Cid) ([]byte, error) {

	df, err := c.getDownloadInfoFromScheduleService(cid)
	if err != nil {
		return nil, err
	}

	if df.URL == "" || df.Token == "" {
		return nil, errors.New("404 Not Found")
	}
	logger.Info("edge ip : ", df.URL)
	return getBlockByHttp(df.URL, df.Token, cid)
}
