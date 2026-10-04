package protocol

// API keys of the requests kgod serves.
const (
	KeyProduce         int16 = 0
	KeyFetch           int16 = 1
	KeyListOffsets     int16 = 2
	KeyMetadata        int16 = 3
	KeyOffsetCommit    int16 = 8
	KeyOffsetFetch     int16 = 9
	KeyFindCoordinator int16 = 10
	KeyJoinGroup       int16 = 11
	KeyHeartbeat       int16 = 12
	KeyLeaveGroup      int16 = 13
	KeySyncGroup       int16 = 14
	KeyApiVersions     int16 = 18
)

// VersionRange is an inclusive range of supported versions.
type VersionRange struct{ Min, Max int16 }

// Supported is the single source for the ApiVersions response and the request gate (ADR 0004).
// Produce 3-7 and Fetch 4-11 are ranges because librdkafka only selects message format v2 when the broker
// advertises Produce v3 and Fetch v4 (see the ledger ruling and ADR 0004).
var Supported = map[int16]VersionRange{
	KeyProduce:         {3, 7},
	KeyFetch:           {4, 11},
	KeyListOffsets:     {2, 2},
	KeyMetadata:        {4, 4},
	KeyOffsetCommit:    {2, 7},
	KeyOffsetFetch:     {1, 7},
	KeyFindCoordinator: {0, 2},
	KeyJoinGroup:       {0, 5},
	KeyHeartbeat:       {0, 3},
	KeyLeaveGroup:      {0, 1},
	KeySyncGroup:       {0, 3},
	KeyApiVersions:     {0, 3},
}

// IsSupported reports whether kgod serves the given API key at the given version.
func IsSupported(key, version int16) bool {
	r, ok := Supported[key]
	return ok && version >= r.Min && version <= r.Max
}

// flexibleFrom is the first flexible (KIP-482) version of each known key.
var flexibleFrom = map[int16]int16{
	KeyProduce:         9,
	KeyFetch:           12,
	KeyListOffsets:     6,
	KeyMetadata:        9,
	KeyOffsetCommit:    8,
	KeyOffsetFetch:     6,
	KeyFindCoordinator: 3,
	KeyJoinGroup:       6,
	KeyHeartbeat:       4,
	KeyLeaveGroup:      4,
	KeySyncGroup:       4,
	KeyApiVersions:     3,
}

// IsFlexible reports whether the version uses flexible encodings (request header v2).
func IsFlexible(key, version int16) bool {
	from, ok := flexibleFrom[key]
	return ok && version >= from
}

// ResponseHeaderVersion returns the response header version: always 0 for ApiVersions,
// 1 for flexible versions of other APIs, else 0.
func ResponseHeaderVersion(key, version int16) int16 {
	if key == KeyApiVersions {
		return 0
	}
	if IsFlexible(key, version) {
		return 1
	}
	return 0
}
