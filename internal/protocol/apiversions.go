package protocol

import "sort"

// ApiVersionsRequest is the ApiVersions request; the names exist from v3.
type ApiVersionsRequest struct{ ClientSoftwareName, ClientSoftwareVersion string }

// Decode reads the request body at the given version.
func (q *ApiVersionsRequest) Decode(r *Reader, version int16) error {
	if version >= 3 {
		q.ClientSoftwareName = r.CompactString()
		q.ClientSoftwareVersion = r.CompactString()
		r.SkipTaggedFields()
	}
	return r.Err()
}

// ApiKeyRange is one supported API and its version range.
type ApiKeyRange struct{ Key, Min, Max int16 }

// ApiVersionsResponse is the ApiVersions response.
type ApiVersionsResponse struct {
	ErrorCode int16
	Keys      []ApiKeyRange // sorted by key
}

// Encode writes the response body at the given version (v3 is flexible).
func (p *ApiVersionsResponse) Encode(w *Writer, version int16) {
	w.Int16(p.ErrorCode)
	if version >= 3 {
		w.CompactArrayLen(len(p.Keys))
		for _, k := range p.Keys {
			w.Int16(k.Key)
			w.Int16(k.Min)
			w.Int16(k.Max)
			w.EmptyTaggedFields()
		}
		w.Int32(0) // throttle_time_ms
		w.EmptyTaggedFields()
		return
	}
	w.ArrayLen(len(p.Keys))
	for _, k := range p.Keys {
		w.Int16(k.Key)
		w.Int16(k.Min)
		w.Int16(k.Max)
	}
	if version >= 1 {
		w.Int32(0) // throttle_time_ms
	}
}

// SupportedKeys returns Supported as a slice sorted by API key.
func SupportedKeys() []ApiKeyRange {
	keys := make([]ApiKeyRange, 0, len(Supported))
	for k, r := range Supported {
		keys = append(keys, ApiKeyRange{Key: k, Min: r.Min, Max: r.Max})
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i].Key < keys[j].Key })
	return keys
}
