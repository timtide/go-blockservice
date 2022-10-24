package blockservice

import (
	"context"
	"fmt"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-blockservice/titan"
	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	ipld "github.com/ipfs/go-ipld-format"
	"sync"
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
		logger.Debugf("got block success from local By cid : %s", c)
		return block, nil
	}

	titanBlock, terr := titan.GetBlockFromTitan(ctx, c)
	if terr == nil {
		logger.Debugf("got block success from titan By cid : %s", c.String())
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
		logger.Debugf("got block success from ipfs network By cid : %s", c)
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

func loadBlocksByLocalTitanIpfs(ctx context.Context, ks []cid.Cid, bs blockstore.Blockstore, fget func() notifiableFetcher, out chan blocks.Block) {
	var misses []cid.Cid
	for _, c := range ks {
		hit, err := bs.Get(ctx, c)
		if err != nil {
			misses = append(misses, c)
			continue
		}
		select {
		case out <- hit:
			logger.Debugf("got block success from local By cid : %s", hit.Cid())
		case <-ctx.Done():
			return
		}
	}

	var wg sync.WaitGroup
	var titanMisses []cid.Cid
	if len(misses) != 0 {
		wg.Add(len(misses))
		for _, c := range misses {
			value := c
			go func(cid cid.Cid) {
				defer wg.Done()
				hit, err := titan.GetBlockFromTitan(ctx, cid)
				if err != nil {
					titanMisses = append(titanMisses, cid)
					return
				}
				select {
				case out <- hit:
					logger.Debugf("got block success from titan By cid : %s", hit.Cid())
				case <-ctx.Done():
					return
				}
			}(value)
		}
	}
	wg.Wait()

	if len(titanMisses) == 0 || fget == nil {
		return
	}

	f := fget() // don't load exchange unless we have to
	rblocks, err := f.GetBlocks(ctx, titanMisses)
	if err != nil {
		logger.Debugf("Error with GetBlocks: %s", err)
		return
	}

	// batch available blocks together
	const batchSize = 32
	batch := make([]blocks.Block, 0, batchSize)
	for {
		var noMoreBlocks bool
	batchLoop:
		for len(batch) < batchSize {
			select {
			case b, ok := <-rblocks:
				if !ok {
					noMoreBlocks = true
					break batchLoop
				}
				logger.Debugf("got block success from ipfs network By cid : %s", b.Cid())
				// logger.Debugf("BlockService.BlockFetched %s", b.Cid())
				batch = append(batch, b)
			case <-ctx.Done():
				return
			default:
				break batchLoop
			}
		}

		// also write in the blockstore for caching, inform the exchange that the blocks are available
		err = bs.PutMany(ctx, batch)
		if err != nil {
			logger.Errorf("could not write blocks from the network to the blockstore: %s", err)
			return
		}

		err = f.NotifyNewBlocks(ctx, batch...)
		if err != nil {
			logger.Errorf("could not tell the exchange about new blocks: %s", err)
			return
		}

		for _, b := range batch {
			select {
			case out <- b:
			case <-ctx.Done():
				return
			}
		}
		batch = batch[:0]
		if noMoreBlocks {
			break
		}
	}
}

// local > titan to load block data
func loadBlockByLocalTitan(ctx context.Context, c cid.Cid, bs blockstore.Blockstore) (blocks.Block, error) {
	block, err := bs.Get(ctx, c)
	if err == nil {
		logger.Debugf("got block success from local By cid : %s", c)
		return block, nil
	}

	if ipld.IsNotFound(err) {
		titanBlock, err := titan.GetBlockFromTitan(ctx, c)
		if err == nil {
			logger.Debugf("got block success from titan By cid : %s", c.String())
			return titanBlock, nil
		}
	}

	logger.Debug("Block service GetBlock: Not found")
	return nil, err
}

func loadBlocksByLocalTitan(ctx context.Context, ks []cid.Cid, bs blockstore.Blockstore, out chan blocks.Block) {
	var misses []cid.Cid
	for _, c := range ks {
		hit, err := bs.Get(ctx, c)
		if err != nil {
			misses = append(misses, c)
			continue
		}
		select {
		case out <- hit:
			logger.Debugf("got block success from local By cid : %s", hit.Cid())
		case <-ctx.Done():
			return
		}
	}

	var wg sync.WaitGroup
	var titanMisses []cid.Cid
	if len(misses) != 0 {
		wg.Add(len(misses))
		for _, c := range misses {
			value := c
			go func(cid cid.Cid) {
				defer wg.Done()
				hit, err := titan.GetBlockFromTitan(ctx, cid)
				if err != nil {
					titanMisses = append(titanMisses, cid)
					return
				}
				select {
				case out <- hit:
					logger.Debugf("got block success from titan By cid : %s", hit.Cid())
				case <-ctx.Done():
					return
				}
			}(value)
		}
	}
	wg.Wait()
}

// local > ipfs to load block data
func loadBlockByLocalIpfs(ctx context.Context, c cid.Cid, bs blockstore.Blockstore, fget func() notifiableFetcher) (blocks.Block, error) {
	block, err := bs.Get(ctx, c)
	if err == nil {
		logger.Debugf("got block success from local By cid : %s", c)
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
		logger.Debugf("got block success from ipfs network By cid : %s", c)
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
func loadBlocksByLocalIpfs(ctx context.Context, ks []cid.Cid, bs blockstore.Blockstore, fget func() notifiableFetcher, out chan blocks.Block) {
	var misses []cid.Cid
	for _, c := range ks {
		hit, err := bs.Get(ctx, c)
		if err != nil {
			misses = append(misses, c)
			continue
		}
		select {
		case out <- hit:
			logger.Debugf("got block success from local By cid : %s", hit.Cid())
		case <-ctx.Done():
			return
		}
	}

	if len(misses) == 0 || fget == nil {
		return
	}

	f := fget() // don't load exchange unless we have to
	rblocks, err := f.GetBlocks(ctx, misses)
	if err != nil {
		logger.Debugf("Error with GetBlocks: %s", err)
		return
	}

	// batch available blocks together
	const batchSize = 32
	batch := make([]blocks.Block, 0, batchSize)
	for {
		var noMoreBlocks bool
	batchLoop:
		for len(batch) < batchSize {
			select {
			case b, ok := <-rblocks:
				if !ok {
					noMoreBlocks = true
					break batchLoop
				}
				logger.Debugf("got block success from ipfs network By cid : %s", b.Cid())
				// logger.Debugf("BlockService.BlockFetched %s", b.Cid())
				batch = append(batch, b)
			case <-ctx.Done():
				return
			default:
				break batchLoop
			}
		}

		// also write in the blockstore for caching, inform the exchange that the blocks are available
		err = bs.PutMany(ctx, batch)
		if err != nil {
			logger.Errorf("could not write blocks from the network to the blockstore: %s", err)
			return
		}

		err = f.NotifyNewBlocks(ctx, batch...)
		if err != nil {
			logger.Errorf("could not tell the exchange about new blocks: %s", err)
			return
		}

		for _, b := range batch {
			select {
			case out <- b:
			case <-ctx.Done():
				return
			}
		}
		batch = batch[:0]
		if noMoreBlocks {
			break
		}
	}
}

// local to load block data
func loadBlockByLocal(ctx context.Context, c cid.Cid, bs blockstore.Blockstore) (blocks.Block, error) {
	block, err := bs.Get(ctx, c)
	if err == nil {
		logger.Debugf("got block success from local By cid : %s", c)
		return block, nil
	}

	logger.Debug("Block service GetBlock: Not found")
	return nil, err
}

func loadBlocksByLocal(ctx context.Context, ks []cid.Cid, bs blockstore.Blockstore, out chan blocks.Block) {
	for _, c := range ks {
		hit, err := bs.Get(ctx, c)
		if err != nil {
			continue
		}
		select {
		case out <- hit:
			logger.Debugf("got block success from local By cid : %s", hit.Cid())
		case <-ctx.Done():
			return
		}
	}
}

// titan to load block data
func loadBlockByTitan(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	titanBlock, terr := titan.GetBlockFromTitan(ctx, c)
	if terr == nil {
		logger.Debugf("got block success from titan By cid : %s", c.String())
		return titanBlock, nil
	}

	logger.Debug("Block service GetBlock: Not found")
	return nil, terr
}

func loadBlocksByTitan(ctx context.Context, ks []cid.Cid, out chan blocks.Block) {
	var wg sync.WaitGroup
	var titanMisses []cid.Cid
	if len(ks) != 0 {
		wg.Add(len(ks))
		for _, c := range ks {
			value := c
			go func(cid cid.Cid) {
				defer wg.Done()
				hit, err := titan.GetBlockFromTitan(ctx, cid)
				if err != nil {
					titanMisses = append(titanMisses, cid)
					return
				}
				select {
				case out <- hit:
					logger.Debugf("got block success from titan By cid : %s", hit.Cid())
				case <-ctx.Done():
					return
				}
			}(value)
		}
	}
	wg.Wait()
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
		logger.Debugf("got block success from ipfs network By cid : %s", c)
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

func loadBlocksByIpfs(ctx context.Context, ks []cid.Cid, bs blockstore.Blockstore, fget func() notifiableFetcher, out chan blocks.Block) {
	if len(ks) == 0 || fget == nil {
		return
	}

	f := fget() // don't load exchange unless we have to
	rblocks, err := f.GetBlocks(ctx, ks)
	if err != nil {
		logger.Debugf("Error with GetBlocks: %s", err)
		return
	}

	// batch available blocks together
	const batchSize = 32
	batch := make([]blocks.Block, 0, batchSize)
	for {
		var noMoreBlocks bool
	batchLoop:
		for len(batch) < batchSize {
			select {
			case b, ok := <-rblocks:
				if !ok {
					noMoreBlocks = true
					break batchLoop
				}
				logger.Debugf("got block success from ipfs network By cid : %s", b.Cid())
				// logger.Debugf("BlockService.BlockFetched %s", b.Cid())
				batch = append(batch, b)
			case <-ctx.Done():
				return
			default:
				break batchLoop
			}
		}

		// also write in the blockstore for caching, inform the exchange that the blocks are available
		err = bs.PutMany(ctx, batch)
		if err != nil {
			logger.Errorf("could not write blocks from the network to the blockstore: %s", err)
			return
		}

		err = f.NotifyNewBlocks(ctx, batch...)
		if err != nil {
			logger.Errorf("could not tell the exchange about new blocks: %s", err)
			return
		}

		for _, b := range batch {
			select {
			case out <- b:
			case <-ctx.Done():
				return
			}
		}
		batch = batch[:0]
		if noMoreBlocks {
			break
		}
	}
}
