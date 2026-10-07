package domain

// Shared wire budgets leave room for validated headers and broker-added fields.
// Response headroom also covers per-record IDs/offsets in a 1,000-record batch.
const (
	MaxPayloadBytes  = 16 << 20
	RecordHeadroom   = 16 << 10
	MaxRequestBytes  = MaxPayloadBytes + RecordHeadroom
	MaxResponseBytes = MaxRequestBytes + (1 << 20)
	FetchBytes       = MaxRequestBytes
)
