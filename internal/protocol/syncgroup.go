package protocol

import "bytes"

// SyncAssignment is one member's assignment in a SyncGroup request.
type SyncAssignment struct {
	MemberID   string
	Assignment []byte
}

// SyncGroupRequest is the SyncGroup v0-v3 request (group instance id from v3).
type SyncGroupRequest struct {
	GroupID         string
	GenerationID    int32
	MemberID        string
	GroupInstanceID *string
	Assignments     []SyncAssignment
}

// Decode reads a SyncGroup request body; byte slices are copied.
func (q *SyncGroupRequest) Decode(r *Reader, version int16) error {
	q.GroupID = r.String()
	q.GenerationID = r.Int32()
	q.MemberID = r.String()
	if version >= 3 {
		q.GroupInstanceID = r.NullableString()
	}
	n := r.ArrayLen()
	for i := 0; i < n && r.Err() == nil; i++ {
		id := r.String()
		a := bytes.Clone(r.Bytes())
		q.Assignments = append(q.Assignments, SyncAssignment{MemberID: id, Assignment: a})
	}
	return r.Err()
}

// SyncGroupResponse is the SyncGroup v0-v3 response.
type SyncGroupResponse struct {
	ErrorCode  int16
	Assignment []byte
}

// Encode writes a SyncGroup response body; throttle exists from v1.
func (p *SyncGroupResponse) Encode(w *Writer, version int16) {
	if version >= 1 {
		w.Int32(0) // throttle_time_ms
	}
	w.Int16(p.ErrorCode)
	w.Bytes(p.Assignment)
}
