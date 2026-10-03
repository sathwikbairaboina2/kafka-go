package protocol

// Kafka error codes used by kgod.
const (
	ErrNone                      int16 = 0
	ErrOffsetOutOfRange          int16 = 1
	ErrCorruptMessage            int16 = 2
	ErrUnknownTopicOrPartition   int16 = 3
	ErrCoordinatorNotAvailable   int16 = 15
	ErrNotCoordinator            int16 = 16
	ErrInvalidTopic              int16 = 17
	ErrIllegalGeneration         int16 = 22
	ErrInconsistentGroupProtocol int16 = 23
	ErrInvalidGroupID            int16 = 24
	ErrUnknownMemberID           int16 = 25
	ErrInvalidSessionTimeout     int16 = 26
	ErrRebalanceInProgress       int16 = 27
	ErrUnsupportedVersion        int16 = 35
	ErrInvalidRequest            int16 = 42
	ErrMemberIDRequired          int16 = 79
	ErrInvalidRecord             int16 = 87
)
