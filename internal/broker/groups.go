package broker

import (
	"context"
	"sort"
	"time"

	"github.com/sathwikbairaboina2/kafka-go/internal/group"
	"github.com/sathwikbairaboina2/kafka-go/internal/protocol"
)

const (
	minSessionTimeout = time.Second
	maxSessionTimeout = 30 * time.Minute
)

// SetCoordinator attaches the group coordinator; without one the group APIs report COORDINATOR_NOT_AVAILABLE.
func (b *Broker) SetCoordinator(c *group.Coordinator) { b.groups = c }

func (b *Broker) findCoordinator(q *protocol.FindCoordinatorRequest) *protocol.FindCoordinatorResponse {
	switch {
	case q.KeyType != 0 || b.groups == nil:
		return &protocol.FindCoordinatorResponse{ErrorCode: protocol.ErrCoordinatorNotAvailable, NodeID: -1, Port: -1}
	case q.Key == "":
		return &protocol.FindCoordinatorResponse{ErrorCode: protocol.ErrInvalidGroupID, NodeID: -1, Port: -1}
	}
	return &protocol.FindCoordinatorResponse{NodeID: b.cfg.NodeID, Host: b.cfg.Host, Port: b.cfg.Port}
}

func (b *Broker) joinGroup(ctx context.Context, clientID string, q *protocol.JoinGroupRequest) (*protocol.JoinGroupResponse, error) {
	fail := func(code int16) (*protocol.JoinGroupResponse, error) {
		return &protocol.JoinGroupResponse{ErrorCode: code, GenerationID: -1, MemberID: q.MemberID}, nil
	}
	if b.groups == nil {
		return fail(protocol.ErrCoordinatorNotAvailable)
	}
	if q.GroupID == "" {
		return fail(protocol.ErrInvalidGroupID)
	}
	session := time.Duration(q.SessionTimeoutMs) * time.Millisecond
	if session < minSessionTimeout || session > maxSessionTimeout {
		return fail(protocol.ErrInvalidSessionTimeout)
	}
	rebalance := time.Duration(q.RebalanceTimeoutMs) * time.Millisecond
	if rebalance <= 0 {
		rebalance = session
	}
	req := group.JoinRequest{
		MemberID: q.MemberID, ClientID: clientID, SessionTimeout: session, RebalanceTimeout: rebalance, ProtocolType: q.ProtocolType,
	}
	for _, p := range q.Protocols {
		req.Protocols = append(req.Protocols, group.Protocol{Name: p.Name, Metadata: p.Metadata})
	}
	res := b.groups.Join(ctx, q.GroupID, req)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	resp := &protocol.JoinGroupResponse{
		ErrorCode: res.Err, GenerationID: res.Generation, ProtocolName: res.Protocol, Leader: res.Leader, MemberID: res.MemberID,
	}
	for _, m := range res.Members {
		resp.Members = append(resp.Members, protocol.JoinMember{MemberID: m.ID, Metadata: m.Metadata})
	}
	return resp, nil
}

func (b *Broker) syncGroup(ctx context.Context, q *protocol.SyncGroupRequest) (*protocol.SyncGroupResponse, error) {
	if b.groups == nil {
		return &protocol.SyncGroupResponse{ErrorCode: protocol.ErrCoordinatorNotAvailable}, nil
	}
	if q.GroupID == "" {
		return &protocol.SyncGroupResponse{ErrorCode: protocol.ErrInvalidGroupID}, nil
	}
	assignments := make(map[string][]byte, len(q.Assignments))
	for _, a := range q.Assignments {
		assignments[a.MemberID] = a.Assignment
	}
	res := b.groups.Sync(ctx, q.GroupID, q.MemberID, q.GenerationID, assignments)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return &protocol.SyncGroupResponse{ErrorCode: res.Err, Assignment: res.Assignment}, nil
}

func (b *Broker) heartbeat(q *protocol.HeartbeatRequest) *protocol.HeartbeatResponse {
	if b.groups == nil {
		return &protocol.HeartbeatResponse{ErrorCode: protocol.ErrCoordinatorNotAvailable}
	}
	return &protocol.HeartbeatResponse{ErrorCode: b.groups.Heartbeat(q.GroupID, q.MemberID, q.GenerationID)}
}

func (b *Broker) leaveGroup(q *protocol.LeaveGroupRequest) *protocol.LeaveGroupResponse {
	if b.groups == nil {
		return &protocol.LeaveGroupResponse{ErrorCode: protocol.ErrCoordinatorNotAvailable}
	}
	return &protocol.LeaveGroupResponse{ErrorCode: b.groups.Leave(q.GroupID, q.MemberID)}
}

func (b *Broker) offsetCommit(q *protocol.OffsetCommitRequest) (*protocol.OffsetCommitResponse, error) {
	resp := &protocol.OffsetCommitResponse{}
	setAll := func(code int16) {
		for _, t := range q.Topics {
			tr := protocol.CommitTopicResponse{Name: t.Name}
			for _, p := range t.Partitions {
				tr.Partitions = append(tr.Partitions, protocol.CommitPartitionResponse{Index: p.Index, ErrorCode: code})
			}
			resp.Topics = append(resp.Topics, tr)
		}
	}
	if b.groups == nil {
		setAll(protocol.ErrCoordinatorNotAvailable)
		return resp, nil
	}
	if q.GroupID == "" {
		setAll(protocol.ErrInvalidGroupID)
		return resp, nil
	}
	now := time.Now().UnixMilli()
	offsets := map[group.OffsetKey]group.Committed{}
	unknown := map[group.OffsetKey]bool{}
	for _, t := range q.Topics {
		for _, p := range t.Partitions {
			k := group.OffsetKey{Group: q.GroupID, Topic: t.Name, Partition: p.Index}
			if _, ok := b.logs.Get(t.Name, p.Index); !ok {
				unknown[k] = true
				continue
			}
			offsets[k] = group.Committed{Offset: p.Offset, LeaderEpoch: p.LeaderEpoch, Metadata: p.Metadata, CommitTimeMs: now}
		}
	}
	code, err := b.groups.Commit(q.GroupID, q.MemberID, q.GenerationID, offsets)
	if err != nil {
		return nil, err
	}
	for _, t := range q.Topics {
		tr := protocol.CommitTopicResponse{Name: t.Name}
		for _, p := range t.Partitions {
			c := code
			if c == protocol.ErrNone && unknown[group.OffsetKey{Group: q.GroupID, Topic: t.Name, Partition: p.Index}] {
				c = protocol.ErrUnknownTopicOrPartition
			}
			tr.Partitions = append(tr.Partitions, protocol.CommitPartitionResponse{Index: p.Index, ErrorCode: c})
		}
		resp.Topics = append(resp.Topics, tr)
	}
	return resp, nil
}

func (b *Broker) offsetFetch(q *protocol.OffsetFetchRequest) *protocol.OffsetFetchResponse {
	resp := &protocol.OffsetFetchResponse{}
	if b.groups == nil {
		resp.ErrorCode = protocol.ErrCoordinatorNotAvailable
		return resp
	}
	if q.GroupID == "" {
		resp.ErrorCode = protocol.ErrInvalidGroupID
		return resp
	}
	empty := ""
	missing := func(i int32) protocol.OffsetFetchPartitionResponse {
		return protocol.OffsetFetchPartitionResponse{Index: i, Offset: -1, LeaderEpoch: -1, Metadata: &empty}
	}
	if q.AllTopics {
		byTopic := map[string][]protocol.OffsetFetchPartitionResponse{}
		for k, c := range b.groups.Fetch(q.GroupID, nil, true) {
			byTopic[k.Topic] = append(byTopic[k.Topic], protocol.OffsetFetchPartitionResponse{
				Index: k.Partition, Offset: c.Offset, LeaderEpoch: c.LeaderEpoch, Metadata: metaOrEmpty(c.Metadata),
			})
		}
		names := make([]string, 0, len(byTopic))
		for n := range byTopic {
			names = append(names, n)
		}
		sort.Strings(names)
		for _, n := range names {
			ps := byTopic[n]
			sort.Slice(ps, func(i, j int) bool { return ps[i].Index < ps[j].Index })
			resp.Topics = append(resp.Topics, protocol.OffsetFetchTopicResponse{Name: n, Partitions: ps})
		}
		return resp
	}
	var keys []group.OffsetKey
	for _, t := range q.Topics {
		for _, p := range t.Partitions {
			keys = append(keys, group.OffsetKey{Group: q.GroupID, Topic: t.Name, Partition: p})
		}
	}
	got := b.groups.Fetch(q.GroupID, keys, false)
	for _, t := range q.Topics {
		tr := protocol.OffsetFetchTopicResponse{Name: t.Name}
		for _, p := range t.Partitions {
			c, ok := got[group.OffsetKey{Group: q.GroupID, Topic: t.Name, Partition: p}]
			if !ok {
				tr.Partitions = append(tr.Partitions, missing(p))
				continue
			}
			tr.Partitions = append(tr.Partitions, protocol.OffsetFetchPartitionResponse{
				Index: p, Offset: c.Offset, LeaderEpoch: c.LeaderEpoch, Metadata: metaOrEmpty(c.Metadata),
			})
		}
		resp.Topics = append(resp.Topics, tr)
	}
	return resp
}

func metaOrEmpty(m *string) *string {
	if m != nil {
		return m
	}
	s := ""
	return &s
}
