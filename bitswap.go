package bitswap

import (
	"context"
	"fmt"

	"github.com/ipfs/go-bitswap/client"
	"github.com/ipfs/go-bitswap/internal/defaults"
	"github.com/ipfs/go-bitswap/message"
	"github.com/ipfs/go-bitswap/network"
	"github.com/ipfs/go-bitswap/server"
	"github.com/ipfs/go-bitswap/tracer"
	"github.com/ipfs/go-metrics-interface"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	exchange "github.com/ipfs/go-ipfs-exchange-interface"
	logging "github.com/ipfs/go-log"
	"github.com/libp2p/go-libp2p-core/peer"

	"go.uber.org/multierr"
)

var log = logging.Logger("bitswap")

// old interface we are targeting
type bitswap interface {
	Close() error
	GetBlock(ctx context.Context, k cid.Cid) (blocks.Block, error)
	GetBlocks(ctx context.Context, keys []cid.Cid) (<-chan blocks.Block, error)
	GetWantBlocks() []cid.Cid
	GetWantHaves() []cid.Cid
	GetWantlist() []cid.Cid
	IsOnline() bool
	LedgerForPeer(p peer.ID) *server.Receipt
	NewSession(ctx context.Context) exchange.Fetcher
	NotifyNewBlocks(ctx context.Context, blks ...blocks.Block) error
	PeerConnected(p peer.ID)
	PeerDisconnected(p peer.ID)
	ReceiveError(err error)
	ReceiveMessage(ctx context.Context, p peer.ID, incoming message.BitSwapMessage)
	Stat() (*Stat, error)
	WantlistForPeer(p peer.ID) []cid.Cid
}

var _ exchange.SessionExchange = (*Bitswap)(nil)
var _ bitswap = (*Bitswap)(nil)
var HasBlockBufferSize = defaults.HasBlockBufferSize

type Bitswap struct {
	*client.Client
	*server.Server

	tracer tracer.Tracer
	net    network.BitSwapNetwork
}

func New(ctx context.Context, net network.BitSwapNetwork, bstore blockstore.Blockstore, options ...Option) *Bitswap {
	bs := &Bitswap{
		net: net,
	}

	var serverOptions []server.Option
	var clientOptions []client.Option

	for _, o := range options {
		switch typedOption := o.v.(type) {
		case server.Option:
			serverOptions = append(serverOptions, typedOption)
		case client.Option:
			clientOptions = append(clientOptions, typedOption)
		case option:
			typedOption(bs)
		default:
			panic(fmt.Errorf("unknown option type passed to bitswap.New, got: %T, %v; expected: %T, %T or %T", typedOption, typedOption, server.Option(nil), client.Option(nil), server.Option(nil)))
		}
	}

	if bs.tracer != nil {
		var tracer tracer.Tracer = nopReceiveTracer{bs.tracer}
		clientOptions = append(clientOptions, client.WithTracer(tracer))
		serverOptions = append(serverOptions, server.WithTracer(tracer))
	}

	if HasBlockBufferSize != defaults.HasBlockBufferSize {
		serverOptions = append(serverOptions, server.HasBlockBufferSize(HasBlockBufferSize))
	}

	ctx = metrics.CtxSubScope(ctx, "bitswap")

	bs.Server = server.New(ctx, net, bstore, serverOptions...)
	bs.Client = client.New(ctx, net, bstore, append(clientOptions, client.WithBlockReceivedNotifier(bs.Server))...)
	net.Start(bs) // use the polyfill receiver to log received errors and trace messages only once

	return bs
}

func (bs *Bitswap) NotifyNewBlocks(ctx context.Context, blks ...blocks.Block) error {
	return multierr.Combine(
		bs.Client.NotifyNewBlocks(ctx, blks...),
		bs.Server.NotifyNewBlocks(ctx, blks...),
	)
}

type Stat struct {
	Wantlist         []cid.Cid
	Peers            []string
	BlocksReceived   uint64
	DataReceived     uint64
	DupBlksReceived  uint64
	DupDataReceived  uint64
	MessagesReceived uint64
	BlocksSent       uint64
	DataSent         uint64
	ProvideBufLen    int
}

/*
// TODO: Some of this stuff really only needs to be done when adding a block
// from the user, not when receiving it from the network.
// In case you run `git blame` on this comment, I'll save you some time: ask
// @whyrusleeping, I don't know the answers you seek.
func (bs *Bitswap) receiveBlocksFrom(ctx context.Context, from peer.ID, blks []blocks.Block, haves []cid.Cid, dontHaves []cid.Cid) error {
	select {
	case <-bs.process.Closing():
		return errors.New("bitswap is closed")
	default:
	}

	wanted := blks

	// If blocks came from the network
	if from != "" {
		var notWanted []blocks.Block
		wanted, notWanted = bs.sim.SplitWantedUnwanted(blks)
		for _, b := range notWanted {
			log.Debugf("[recv] block not in wantlist; cid=%s, peer=%s", b.Cid(), from)
		}
	}

	for _, b := range wanted {
		fmt.Println("[Print Debug] wanted: " + b.Cid().GetRequest())
	}

	// Put wanted blocks into blockstore
	if len(wanted) > 0 {
		err := bs.blockstore.PutMany(ctx, wanted)
		if err != nil {
			log.Errorf("Error writing %d blocks to datastore: %s", len(wanted), err)
			return err
		}
	}

	// NOTE: There exists the possiblity for a race condition here.  If a user
	// creates a node, then adds it to the dagservice while another goroutine
	// is waiting on a GetBlock for that object, they will receive a reference
	// to the same node. We should address this soon, but i'm not going to do
	// it now as it requires more thought and isnt causing immediate problems.

	allKs := make([]cid.Cid, 0, len(blks))
	for _, b := range blks {
		allKs = append(allKs, b.Cid())
	}

	// If the message came from the network
	if from != "" {
		// Inform the PeerManager so that we can calculate per-peer latency
		combined := make([]cid.Cid, 0, len(allKs)+len(haves)+len(dontHaves))
		combined = append(combined, allKs...)
		combined = append(combined, haves...)
		combined = append(combined, dontHaves...)
		bs.pm.ResponseReceived(from, combined)
	}

	// Send all block keys (including duplicates) to any sessions that want them.
	// (The duplicates are needed by sessions for accounting purposes)
	bs.sm.ReceiveFrom(ctx, from, allKs, haves, dontHaves)

	// Send wanted blocks to decision engine
	bs.engine.ReceiveFrom(from, wanted)

	// Publish the block to any Bitswap clients that had requested blocks.
	// (the sessions use this pubsub mechanism to inform clients of incoming
	// blocks)
	for _, b := range wanted {
		bs.notif.Publish(b)
	}

	// If the reprovider is enabled, send wanted blocks to reprovider
	if bs.provideEnabled {
		for _, blk := range wanted {
			select {
			case bs.newBlocks <- blk.Cid():
				// send block off to be reprovided
			case <-bs.process.Closing():
				return bs.process.Close()
			}
		}
	}

	if from != "" {
		for _, b := range wanted {
			log.Debugw("Bitswap.GetBlockRequest.End", "cid", b.Cid())
		}
	}

	return nil
}
*/

func (bs *Bitswap) Stat() (*Stat, error) {
	cs, err := bs.Client.Stat()
	if err != nil {
		return nil, err
	}
	ss, err := bs.Server.Stat()
	if err != nil {
		return nil, err
	}

	return &Stat{
		Wantlist:         cs.Wantlist,
		BlocksReceived:   cs.BlocksReceived,
		DataReceived:     cs.DataReceived,
		DupBlksReceived:  cs.DupBlksReceived,
		DupDataReceived:  cs.DupDataReceived,
		MessagesReceived: cs.MessagesReceived,
		Peers:            ss.Peers,
		BlocksSent:       ss.BlocksSent,
		DataSent:         ss.DataSent,
		ProvideBufLen:    ss.ProvideBufLen,
	}, nil
}

func (bs *Bitswap) Close() error {
	bs.net.Stop()
	return multierr.Combine(
		bs.Client.Close(),
		bs.Server.Close(),
	)
}

func (bs *Bitswap) WantlistForPeer(p peer.ID) []cid.Cid {
	if p == bs.net.Self() {
		return bs.Client.GetWantlist()
	}
	return bs.Server.WantlistForPeer(p)
}

func (bs *Bitswap) PeerConnected(p peer.ID) {
	bs.Client.PeerConnected(p)
	bs.Server.PeerConnected(p)
}

func (bs *Bitswap) PeerDisconnected(p peer.ID) {
	bs.Client.PeerDisconnected(p)
	bs.Server.PeerDisconnected(p)
}

func (bs *Bitswap) ReceiveError(err error) {
	log.Infof("Bitswap Client ReceiveError: %s", err)
	// TODO log the network error
	// TODO bubble the network error up to the parent context/error logger
}

func (bs *Bitswap) ReceiveMessage(ctx context.Context, p peer.ID, incoming message.BitSwapMessage) {
	if bs.tracer != nil {
		bs.tracer.MessageReceived(p, incoming)
	}

	bs.Client.ReceiveMessage(ctx, p, incoming)
	bs.Server.ReceiveMessage(ctx, p, incoming)
}
