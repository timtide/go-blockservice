package titan

import (
	"context"
	"errors"
	"github.com/ipfs/go-cid"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/api/client"
)

type ClientOfTitan struct {
	ctx          context.Context
	SchedulerURL string
}

func NewClientTitan(ctx context.Context, url string) *ClientOfTitan {
	return &ClientOfTitan{
		ctx:          ctx,
		SchedulerURL: url,
	}
}

func (c *ClientOfTitan) getDownloadInfoFromScheduleService(cid cid.Cid) (*api.DownloadInfo, error) {
	apiScheduler, closer, err := client.NewScheduler(c.ctx, c.SchedulerURL, nil)
	if err != nil {
		return nil, err
	}
	defer closer()
	str := cid.String()
	downloadInfo, err := apiScheduler.GetDownloadInfoWithBlock(c.ctx, str, "")
	if err != nil {
		return nil, err
	}
	return &downloadInfo, nil
}

func (c *ClientOfTitan) GetDataFromEdgeNode(cid cid.Cid) ([]byte, error) {

	df, err := c.getDownloadInfoFromScheduleService(cid)
	if err != nil {
		return nil, err
	}

	if df.URL == "" || df.Token == "" {
		return nil, errors.New("404 Not Found")
	}

	return GetBlockByHttp(df.URL, df.Token, cid)
}
