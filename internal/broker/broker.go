// Package broker decodes each supported request, calls storage, metadata or the group coordinator,
// and encodes the response. It implements server.Handler.
package broker

import (
	"context"
	"fmt"

	"github.com/sathwikbairaboina2/kafka-go/internal/group"
	"github.com/sathwikbairaboina2/kafka-go/internal/meta"
	"github.com/sathwikbairaboina2/kafka-go/internal/protocol"
	"github.com/sathwikbairaboina2/kafka-go/internal/storage"
)

// errUnknownServer is Kafka's UNKNOWN_SERVER_ERROR.
const errUnknownServer int16 = -1

// Config is the broker identity and topic creation policy.
type Config struct {
	NodeID            int32  // 1
	Host              string // advertised
	Port              int32  // advertised
	ClusterID         string // "kafka-go"
	AutoCreate        bool
	DefaultPartitions int32
}

// Broker serves the supported Kafka APIs for a single node.
type Broker struct {
	cfg    Config
	meta   *meta.Store
	logs   *storage.Manager
	groups *group.Coordinator
}

// New returns a Broker over the given topic store and log manager.
func New(cfg Config, ms *meta.Store, lm *storage.Manager) *Broker {
	if cfg.DefaultPartitions < 1 {
		cfg.DefaultPartitions = 1
	}
	return &Broker{cfg: cfg, meta: ms, logs: lm}
}

// ensureTopic returns the topic, creating it in meta and storage when allowAuto permits.
// The returned code is 0 on success.
func (b *Broker) ensureTopic(name string, allowAuto bool) (meta.Topic, int16) {
	if !meta.ValidName(name) {
		return meta.Topic{}, protocol.ErrInvalidTopic
	}
	t, ok := b.meta.Get(name)
	if !ok {
		if !allowAuto {
			return meta.Topic{}, protocol.ErrUnknownTopicOrPartition
		}
		var err error
		if t, _, err = b.meta.Create(name, b.cfg.DefaultPartitions); err != nil {
			return meta.Topic{}, errUnknownServer
		}
	}
	if err := b.logs.Ensure(t.Name, t.Partitions); err != nil {
		return meta.Topic{}, errUnknownServer
	}
	return t, protocol.ErrNone
}

// Handle implements server.Handler.
func (b *Broker) Handle(ctx context.Context, h protocol.RequestHeader, r *protocol.Reader) ([]byte, bool, error) {
	w := protocol.NewWriter(256)
	switch h.APIKey {
	case protocol.KeyApiVersions:
		var q protocol.ApiVersionsRequest
		if err := q.Decode(r, h.APIVersion); err != nil {
			return nil, false, fmt.Errorf("decode ApiVersions: %w", err)
		}
		(&protocol.ApiVersionsResponse{Keys: protocol.SupportedKeys()}).Encode(w, h.APIVersion)
	case protocol.KeyMetadata:
		var q protocol.MetadataRequest
		if err := q.Decode(r, h.APIVersion); err != nil {
			return nil, false, fmt.Errorf("decode Metadata: %w", err)
		}
		b.metadata(&q).Encode(w, h.APIVersion)
	case protocol.KeyProduce:
		var q protocol.ProduceRequest
		if err := q.Decode(r, h.APIVersion); err != nil {
			return nil, false, fmt.Errorf("decode Produce: %w", err)
		}
		resp := b.produce(&q)
		if q.Acks == 0 {
			return nil, false, nil
		}
		resp.Encode(w, h.APIVersion)
	case protocol.KeyFetch:
		var q protocol.FetchRequest
		if err := q.Decode(r, h.APIVersion); err != nil {
			return nil, false, fmt.Errorf("decode Fetch: %w", err)
		}
		b.fetch(ctx, &q).Encode(w, h.APIVersion)
	case protocol.KeyListOffsets:
		var q protocol.ListOffsetsRequest
		if err := q.Decode(r, h.APIVersion); err != nil {
			return nil, false, fmt.Errorf("decode ListOffsets: %w", err)
		}
		b.listOffsets(&q).Encode(w, h.APIVersion)
	case protocol.KeyFindCoordinator:
		var q protocol.FindCoordinatorRequest
		if err := q.Decode(r, h.APIVersion); err != nil {
			return nil, false, fmt.Errorf("decode FindCoordinator: %w", err)
		}
		b.findCoordinator(&q).Encode(w, h.APIVersion)
	case protocol.KeyJoinGroup:
		var q protocol.JoinGroupRequest
		if err := q.Decode(r, h.APIVersion); err != nil {
			return nil, false, fmt.Errorf("decode JoinGroup: %w", err)
		}
		client := ""
		if h.ClientID != nil {
			client = *h.ClientID
		}
		resp, err := b.joinGroup(ctx, client, h.APIVersion, &q)
		if err != nil {
			return nil, false, err
		}
		resp.Encode(w, h.APIVersion)
	case protocol.KeySyncGroup:
		var q protocol.SyncGroupRequest
		if err := q.Decode(r, h.APIVersion); err != nil {
			return nil, false, fmt.Errorf("decode SyncGroup: %w", err)
		}
		resp, err := b.syncGroup(ctx, &q)
		if err != nil {
			return nil, false, err
		}
		resp.Encode(w, h.APIVersion)
	case protocol.KeyHeartbeat:
		var q protocol.HeartbeatRequest
		if err := q.Decode(r, h.APIVersion); err != nil {
			return nil, false, fmt.Errorf("decode Heartbeat: %w", err)
		}
		b.heartbeat(&q).Encode(w, h.APIVersion)
	case protocol.KeyLeaveGroup:
		var q protocol.LeaveGroupRequest
		if err := q.Decode(r, h.APIVersion); err != nil {
			return nil, false, fmt.Errorf("decode LeaveGroup: %w", err)
		}
		b.leaveGroup(&q).Encode(w, h.APIVersion)
	case protocol.KeyOffsetCommit:
		var q protocol.OffsetCommitRequest
		if err := q.Decode(r, h.APIVersion); err != nil {
			return nil, false, fmt.Errorf("decode OffsetCommit: %w", err)
		}
		resp, err := b.offsetCommit(&q)
		if err != nil {
			return nil, false, err
		}
		resp.Encode(w, h.APIVersion)
	case protocol.KeyOffsetFetch:
		var q protocol.OffsetFetchRequest
		if err := q.Decode(r, h.APIVersion); err != nil {
			return nil, false, fmt.Errorf("decode OffsetFetch: %w", err)
		}
		b.offsetFetch(&q).Encode(w, h.APIVersion)
	default:
		return nil, false, fmt.Errorf("api key %d not implemented", h.APIKey)
	}
	return w.Buf(), true, nil
}
