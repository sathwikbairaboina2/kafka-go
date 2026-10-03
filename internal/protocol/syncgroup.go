package protocol

import "bytes"

// SyncAssignment is one member's assignment in a SyncGroup request.
type SyncAssignment struct {
	MemberID   string
	Assignment []byte
}

// SyncGroupRequest is the SyncGroup v3 request.
type SyncGroupRequest struct {
	GroupID         string
	GenerationID    int32
	MemberID        string
	GroupInstanceID *string
	Assignments     []SyncAssignment
}

// Decode reads a SyncGroup v3 request body; byte slices are copied.
func (q *SyncGroupRequest) Decode(r *Reader, version int16) error {
	q.GroupID = r.String()
	q.GenerationID = r.Int32()
	q.MemberID = r.String()
	q.GroupInstanceID = r.NullableString()
	n := r.ArrayLen()
	for i := 0; i < n && r.Err() == nil; i++ {
		id := r.String()
		a := bytes.Clone(r.Bytes())
		q.Assignments = append(q.Assignments, SyncAssignment{MemberID: id, Assignment: a})
	}
	return r.Err()
}

// SyncGroupResponse is the SyncGroup v3 response.
type SyncGroupResponse struct {
	ErrorCode  int16
	Assignment []byte
}

// Encode writes a SyncGroup v3 response body.
func (p *SyncGroupResponse) Encode(w *Writer, version int16) {
	w.Int32(0) // throttle_time_ms
	w.Int16(p.ErrorCode)
	w.Bytes(p.Assignment)
}
