// package blockservice implements a BlockService interface that provides
// a single GetBlock/AddBlock interface that seamlessly retrieves data either
// locally or from a remote peer through the exchange.
package blockservice

import (
	"context"
	"fmt"
	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	exchange "github.com/ipfs/go-ipfs-exchange-interface"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipfs/go-verifcid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"io"
	"sync"

	"github.com/ipfs/go-blockservice/internal"
)

var logger = logging.Logger("blockservice")

// BlockGetter is the common interface shared between blockservice sessions and
// the blockservice.
type BlockGetter interface {
	// GetBlock gets the requested block.
	GetBlock(ctx context.Context, c cid.Cid) (blocks.Block, error)

	// GetBlocks does a batch request for the given cids, returning blocks as
	// they are found, in no particular order.
	//
	// It may not be able to find all requested blocks (or the context may
	// be canceled). In that case, it will close the channel early. It is up
	// to the consumer to detect this situation and keep track which blocks
	// it has received and which it hasn't.
	GetBlocks(ctx context.Context, ks []cid.Cid) <-chan blocks.Block
}

// BlockService is a hybrid block datastore. It stores data in a local
// datastore and may retrieve data from a remote Exchange.
// It uses an internal `datastore.Datastore` instance to store values.
type BlockService interface {
	io.Closer
	BlockGetter

	// Blockstore returns a reference to the underlying blockstore
	Blockstore() blockstore.Blockstore

	// Exchange returns a reference to the underlying exchange (usually bitswap)
	Exchange() exchange.Interface

	// AddBlock puts a given block to the underlying datastore
	AddBlock(ctx context.Context, o blocks.Block) error

	// AddBlocks adds a slice of blocks at the same time using batching
	// capabilities of the underlying datastore whenever possible.
	AddBlocks(ctx context.Context, bs []blocks.Block) error

	// DeleteBlock deletes the given block from the blockservice.
	DeleteBlock(ctx context.Context, o cid.Cid) error
}

type blockService struct {
	blockstore blockstore.Blockstore
	exchange   exchange.Interface
	// If checkFirst is true then first check that a block doesn't
	// already exist to avoid republishing the block on the exchange.
	checkFirst bool
}

// NewBlockService creates a BlockService with given datastore instance.
func New(bs blockstore.Blockstore, rem exchange.Interface) BlockService {
	if rem == nil {
		logger.Debug("blockservice running in local (offline) mode.")
	}

	return &blockService{
		blockstore: bs,
		exchange:   rem,
		checkFirst: true,
	}
}

// NewWriteThrough creates a BlockService that guarantees writes will go
// through to the blockstore and are not skipped by cache checks.
func NewWriteThrough(bs blockstore.Blockstore, rem exchange.Interface) BlockService {
	if rem == nil {
		logger.Debug("blockservice running in local (offline) mode.")
	}

	return &blockService{
		blockstore: bs,
		exchange:   rem,
		checkFirst: false,
	}
}

// Blockstore returns the blockstore behind this blockservice.
func (s *blockService) Blockstore() blockstore.Blockstore {
	return s.blockstore
}

// Exchange returns the exchange behind this blockservice.
func (s *blockService) Exchange() exchange.Interface {
	return s.exchange
}

// NewSession creates a new session that allows for
// controlled exchange of wantlists to decrease the bandwidth overhead.
// If the current exchange is a SessionExchange, a new exchange
// session will be created. Otherwise, the current exchange will be used
// directly.
func NewSession(ctx context.Context, bs BlockService) *Session {
	exch := bs.Exchange()
	if sessEx, ok := exch.(exchange.SessionExchange); ok {
		return &Session{
			sessCtx:  ctx,
			ses:      nil,
			sessEx:   sessEx,
			bs:       bs.Blockstore(),
			notifier: exch,
		}
	}
	return &Session{
		ses:      exch,
		sessCtx:  ctx,
		bs:       bs.Blockstore(),
		notifier: exch,
	}
}

// AddBlock adds a particular block to the service, Putting it into the datastore.
func (s *blockService) AddBlock(ctx context.Context, o blocks.Block) error {
	ctx, span := internal.StartSpan(ctx, "blockService.AddBlock")
	defer span.End()

	c := o.Cid()
	// hash security
	err := verifcid.ValidateCid(c)
	if err != nil {
		return err
	}
	if s.checkFirst {
		if has, err := s.blockstore.Has(ctx, c); has || err != nil {
			return err
		}
	}

	if err := s.blockstore.Put(ctx, o); err != nil {
		return err
	}

	logger.Debugf("BlockService.BlockAdded %s", c)

	if s.exchange != nil {
		if err := s.exchange.NotifyNewBlocks(ctx, o); err != nil {
			logger.Errorf("NotifyNewBlocks: %s", err.Error())
		}
	}

	return nil
}

func (s *blockService) AddBlocks(ctx context.Context, bs []blocks.Block) error {
	ctx, span := internal.StartSpan(ctx, "blockService.AddBlocks")
	defer span.End()

	// hash security
	for _, b := range bs {
		err := verifcid.ValidateCid(b.Cid())
		if err != nil {
			return err
		}
	}
	var toput []blocks.Block
	if s.checkFirst {
		toput = make([]blocks.Block, 0, len(bs))
		for _, b := range bs {
			has, err := s.blockstore.Has(ctx, b.Cid())
			if err != nil {
				return err
			}
			if !has {
				toput = append(toput, b)
			}
		}
	} else {
		toput = bs
	}

	if len(toput) == 0 {
		return nil
	}

	err := s.blockstore.PutMany(ctx, toput)
	if err != nil {
		return err
	}

	if s.exchange != nil {
		logger.Debugf("BlockService.BlockAdded %d blocks", len(toput))
		if err := s.exchange.NotifyNewBlocks(ctx, toput...); err != nil {
			logger.Errorf("NotifyNewBlocks: %s", err.Error())
		}
	}
	return nil
}

// GetBlock retrieves a particular block from the service,
// Getting it from the datastore using the key (hash).
func (s *blockService) GetBlock(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	ctx, span := internal.StartSpan(ctx, "blockService.GetBlock", trace.WithAttributes(attribute.Stringer("CID", c)))
	defer span.End()

	var f func() notifiableFetcher
	if s.exchange != nil {
		f = s.getExchange
	}

	return getBlock(ctx, c, s.blockstore, f) // hash security
}

func (s *blockService) getExchange() notifiableFetcher {
	return s.exchange
}

func getBlock(ctx context.Context, c cid.Cid, bs blockstore.Blockstore, fget func() notifiableFetcher) (blocks.Block, error) {
	err := verifcid.ValidateCid(c) // hash security
	if err != nil {
		return nil, err
	}

	loadLevelInf := ctx.Value(LoadLevelOfSign)
	// other default
	if loadLevelInf == nil {
		loadLevelInf = LoadOfLocalTitanIpfs.Uint8()
	}
	if loadLevel, ok := loadLevelInf.(uint8); ok {
		switch loadLevel {
		case LoadOfLocalTitanIpfs.Uint8():
			return loadBlockByLocalTitanIpfs(ctx, c, bs, fget)
		case LoadOfLocalTitan.Uint8():
			return loadBlockByLocalTitan(ctx, c, bs)
		case LoadOfLocalIpfs.Uint8():
			return loadBlockByLocalIpfs(ctx, c, bs, fget)
		case LoadOfOnlyLocal.Uint8():
			return loadBlockByLocal(ctx, c, bs)
		case LoadOfOnlyTitan.Uint8():
			return loadBlockByTitan(ctx, c)
		case LoadOfOnlyIpfs.Uint8():
			return loadBlockByIpfs(ctx, c, bs, fget)
		default:
			return nil, fmt.Errorf("unknown load level")
		}
	}

	return nil, fmt.Errorf("load level type fail")
}

// GetBlocks gets a list of blocks asynchronously and returns through
// the returned channel.
// NB: No guarantees are made about order.
func (s *blockService) GetBlocks(ctx context.Context, ks []cid.Cid) <-chan blocks.Block {
	ctx, span := internal.StartSpan(ctx, "blockService.GetBlocks")
	defer span.End()

	var f func() notifiableFetcher
	if s.exchange != nil {
		f = s.getExchange
	}
	return getBlocks(ctx, ks, s.blockstore, f) // hash security
}

func getBlocks(ctx context.Context, ks []cid.Cid, bs blockstore.Blockstore, fget func() notifiableFetcher) <-chan blocks.Block {
	out := make(chan blocks.Block)

	go func() {
		defer close(out)

		allValid := true
		for _, c := range ks {
			if err := verifcid.ValidateCid(c); err != nil {
				allValid = false
				break
			}
		}

		if !allValid {
			ks2 := make([]cid.Cid, 0, len(ks))
			for _, c := range ks {
				// hash security
				if err := verifcid.ValidateCid(c); err == nil {
					ks2 = append(ks2, c)
				} else {
					logger.Errorf("unsafe CID (%s) passed to blockService.GetBlocks: %s", c, err)
				}
			}
			ks = ks2
		}

		loadLevelInf := ctx.Value(LoadLevelOfSign)
		// other default
		if loadLevelInf == nil {
			loadLevelInf = LoadOfLocalTitanIpfs.Uint8()
		}
		if loadLevel, ok := loadLevelInf.(uint8); ok {
			switch loadLevel {
			case LoadOfLocalTitanIpfs.Uint8():
				loadBlocksByLocalTitanIpfs(ctx, ks, bs, fget, out)
			case LoadOfLocalTitan.Uint8():
				loadBlocksByLocalTitan(ctx, ks, bs, out)
			case LoadOfLocalIpfs.Uint8():
				loadBlocksByLocalIpfs(ctx, ks, bs, fget, out)
			case LoadOfOnlyLocal.Uint8():
				loadBlocksByLocal(ctx, ks, bs, out)
			case LoadOfOnlyTitan.Uint8():
				loadBlocksByTitan(ctx, ks, out)
			case LoadOfOnlyIpfs.Uint8():
				loadBlocksByIpfs(ctx, ks, bs, fget, out)
			default:
				return
			}
		}
	}()
	return out
}

// DeleteBlock deletes a block in the blockservice from the datastore
func (s *blockService) DeleteBlock(ctx context.Context, c cid.Cid) error {
	ctx, span := internal.StartSpan(ctx, "blockService.DeleteBlock", trace.WithAttributes(attribute.Stringer("CID", c)))
	defer span.End()

	err := s.blockstore.DeleteBlock(ctx, c)
	if err == nil {
		logger.Debugf("BlockService.BlockDeleted %s", c)
	}
	return err
}

func (s *blockService) Close() error {
	logger.Debug("blockservice is shutting down...")
	return s.exchange.Close()
}

type notifier interface {
	NotifyNewBlocks(context.Context, ...blocks.Block) error
}

// Session is a helper type to provide higher level access to bitswap sessions
type Session struct {
	bs       blockstore.Blockstore
	ses      exchange.Fetcher
	sessEx   exchange.SessionExchange
	sessCtx  context.Context
	notifier notifier
	lk       sync.Mutex
}

type notifiableFetcher interface {
	exchange.Fetcher
	notifier
}

type notifiableFetcherWrapper struct {
	exchange.Fetcher
	notifier
}

func (s *Session) getSession() notifiableFetcher {
	s.lk.Lock()
	defer s.lk.Unlock()
	if s.ses == nil {
		s.ses = s.sessEx.NewSession(s.sessCtx)
	}

	return notifiableFetcherWrapper{s.ses, s.notifier}
}

func (s *Session) getExchange() notifiableFetcher {
	return notifiableFetcherWrapper{s.ses, s.notifier}
}

func (s *Session) getFetcherFactory() func() notifiableFetcher {
	if s.sessEx != nil {
		return s.getSession
	}
	if s.ses != nil {
		// Our exchange isn't session compatible, let's fallback to non sessions fetches
		return s.getExchange
	}
	return nil
}

// GetBlock gets a block in the context of a request session
func (s *Session) GetBlock(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	ctx, span := internal.StartSpan(ctx, "Session.GetBlock", trace.WithAttributes(attribute.Stringer("CID", c)))
	defer span.End()

	return getBlock(ctx, c, s.bs, s.getFetcherFactory()) // hash security
}

// GetBlocks gets blocks in the context of a request session
func (s *Session) GetBlocks(ctx context.Context, ks []cid.Cid) <-chan blocks.Block {
	ctx, span := internal.StartSpan(ctx, "Session.GetBlocks")
	defer span.End()

	return getBlocks(ctx, ks, s.bs, s.getFetcherFactory()) // hash security
}

var _ BlockGetter = (*Session)(nil)
