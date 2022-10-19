package blockservice

import (
	"context"
	"fmt"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-blockservice/titan"
	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	ipld "github.com/ipfs/go-ipld-format"
)

const LoadLevelOfSign = "loadLevelOfSign"

type LoadLevel uint8

// load mode local/titan/ipfs network

const (
	LoadOfLocalTitanIpfs LoadLevel = iota // local > titan > ipfs
	LoadOfLocalTitan                      // local > titan
	LoadOfLocalIpfs                       // local > ipfs
	LoadOfOnlyLocal                       // local
	LoadOfOnlyTitan                       // titan
	LoadOfOnlyIpfs                        // ipfs
)

func (l LoadLevel) Uint8() uint8 {
	return uint8(l)
}

func (l LoadLevel) Int() int {
	return int(l)
}

// local > titan > ipfs to load block data
func loadBlockByLocalTitanIpfs(ctx context.Context, c cid.Cid, bs blockstore.Blockstore, fget func() notifiableFetcher) (blocks.Block, error) {
	block, err := bs.Get(ctx, c)
	if err == nil {
		return block, nil
	}

	titanBlock, terr := titan.GetBlockFromTitan(ctx, c)
	if terr == nil {
		logger.Infof("get block success from titan By cid : %s", c.String())
		return titanBlock, nil
	}

	if ipld.IsNotFound(err) && fget != nil {
		f := fget() // Don't load the exchange until we have to

		// TODO be careful checking ErrNotFound. If the underlying
		// implementation changes, this will break.
		logger.Debug("Block service: Searching bitswap")
		blk, err := f.GetBlock(ctx, c)
		if err != nil {
			return nil, err
		}
		// also write in the block store for caching, inform the exchange that the block is available
		err = bs.Put(ctx, blk)
		if err != nil {
			return nil, err
		}
		err = f.NotifyNewBlocks(ctx, blk)
		if err != nil {
			return nil, err
		}
		logger.Debugf("BlockService.BlockFetched %s", c)
		return blk, nil
	}

	logger.Debug("Block service GetBlock: Not found")
	return nil, err
}

// local > titan to load block data
func loadBlockByLocalTitan(ctx context.Context, c cid.Cid, bs blockstore.Blockstore) (blocks.Block, error) {
	block, err := bs.Get(ctx, c)
	if err == nil {
		return block, nil
	}

	titanBlock, terr := titan.GetBlockFromTitan(ctx, c)
	if terr == nil {
		logger.Infof("get block success from titan By cid : %s", c.String())
		return titanBlock, nil
	}

	logger.Debug("Block service GetBlock: Not found")
	return nil, err
}

// local > ipfs to load block data
func loadBlockByLocalIpfs(ctx context.Context, c cid.Cid, bs blockstore.Blockstore, fget func() notifiableFetcher) (blocks.Block, error) {
	block, err := bs.Get(ctx, c)
	if err == nil {
		return block, nil
	}

	if ipld.IsNotFound(err) && fget != nil {
		f := fget() // Don't load the exchange until we have to

		// TODO be careful checking ErrNotFound. If the underlying
		// implementation changes, this will break.
		logger.Debug("Block service: Searching bitswap")
		blk, err := f.GetBlock(ctx, c)
		if err != nil {
			return nil, err
		}
		// also write in the block store for caching, inform the exchange that the block is available
		err = bs.Put(ctx, blk)
		if err != nil {
			return nil, err
		}
		err = f.NotifyNewBlocks(ctx, blk)
		if err != nil {
			return nil, err
		}
		logger.Debugf("BlockService.BlockFetched %s", c)
		return blk, nil
	}

	logger.Debug("Block service GetBlock: Not found")
	return nil, err
}

// local to load block data
func loadBlockByLocal(ctx context.Context, c cid.Cid, bs blockstore.Blockstore) (blocks.Block, error) {
	block, err := bs.Get(ctx, c)
	if err == nil {
		return block, nil
	}

	logger.Debug("Block service GetBlock: Not found")
	return nil, err
}

// titan to load block data
func loadBlockByTitan(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	titanBlock, terr := titan.GetBlockFromTitan(ctx, c)
	if terr == nil {
		logger.Infof("get block success from titan By cid : %s", c.String())
		return titanBlock, nil
	}

	logger.Debug("Block service GetBlock: Not found")
	return nil, terr
}

// ipfs to load block data
func loadBlockByIpfs(ctx context.Context, c cid.Cid, bs blockstore.Blockstore, fget func() notifiableFetcher) (blocks.Block, error) {
	if fget != nil {
		f := fget() // Don't load the exchange until we have to

		// TODO be careful checking ErrNotFound. If the underlying
		// implementation changes, this will break.
		logger.Debug("Block service: Searching bitswap")
		blk, err := f.GetBlock(ctx, c)
		if err != nil {
			return nil, err
		}
		// also write in the block store for caching, inform the exchange that the block is available
		err = bs.Put(ctx, blk)
		if err != nil {
			return nil, err
		}
		err = f.NotifyNewBlocks(ctx, blk)
		if err != nil {
			return nil, err
		}
		logger.Debugf("BlockService.BlockFetched %s", c)
		return blk, nil
	}
	logger.Debug("Block service GetBlock: Not found")
	return nil, fmt.Errorf("notifiable fetcher is null")
}
