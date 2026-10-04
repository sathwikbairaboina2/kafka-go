package protocol

import "bytes"

// JoinProtocol is one assignment protocol a member supports.
type JoinProtocol struct {
	Name     string
	Metadata []byte
}

// JoinGroupRequest is the JoinGroup v0-v5 request. The rebalance timeout exists from v1 (v0 reuses the
// session timeout) and the group instance id from v5.
type JoinGroupRequest struct {
	GroupID                              string
	SessionTimeoutMs, RebalanceTimeoutMs int32
	MemberID                             string
	GroupInstanceID                      *string
	ProtocolType                         string
	Protocols                            []JoinProtocol
}

// Decode reads a JoinGroup request body; byte slices are copied so the group may keep them.
func (q *JoinGroupRequest) Decode(r *Reader, version int16) error {
	q.GroupID = r.String()
	q.SessionTimeoutMs = r.Int32()
	if version >= 1 {
		q.RebalanceTimeoutMs = r.Int32()
	} else {
		q.RebalanceTimeoutMs = q.SessionTimeoutMs
	}
	q.MemberID = r.String()
	if version >= 5 {
		q.GroupInstanceID = r.NullableString()
	}
	q.ProtocolType = r.String()
	n := r.ArrayLen()
	for i := 0; i < n && r.Err() == nil; i++ {
		name := r.String()
		meta := bytes.Clone(r.Bytes())
		q.Protocols = append(q.Protocols, JoinProtocol{Name: name, Metadata: meta})
	}
	return r.Err()
}

// JoinMember is one member in a JoinGroup response (leader only).
type JoinMember struct {
	MemberID string
	Metadata []byte
}

// JoinGroupResponse is the JoinGroup v0-v5 response.
type JoinGroupResponse struct {
	ErrorCode                      int16
	GenerationID                   int32
	ProtocolName, Leader, MemberID string
	Members                        []JoinMember
}

// Encode writes a JoinGroup response body; throttle exists from v2, member instance ids from v5.
func (p *JoinGroupResponse) Encode(w *Writer, version int16) {
	if version >= 2 {
		w.Int32(0) // throttle_time_ms
	}
	w.Int16(p.ErrorCode)
	w.Int32(p.GenerationID)
	w.String(p.ProtocolName)
	w.String(p.Leader)
	w.String(p.MemberID)
	w.ArrayLen(len(p.Members))
	for _, m := range p.Members {
		w.String(m.MemberID)
		if version >= 5 {
			w.NullableString(nil) // group_instance_id
		}
		w.Bytes(m.Metadata)
	}
}
