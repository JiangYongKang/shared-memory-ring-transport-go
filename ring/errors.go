package ring

// Sentinel errors. Each mount rejection has a distinguishable cause.
var (
	ErrInvalidParam     = errInvalidParam{}
	ErrCapacityConflict = errCapacityConflict{}
	ErrVersionConflict  = errVersionConflict{}
	ErrMagicConflict    = errMagicConflict{}
	ErrFormatConflict   = errFormatConflict{}
	ErrBufferFull       = errBufferFull{}
	ErrBufferEmpty      = errBufferEmpty{}
	ErrRecordTooLarge   = errRecordTooLarge{}
	ErrCorruptRecord    = errCorruptRecord{}
	ErrNotOpen          = errNotOpen{}
)

type errInvalidParam struct{}
type errCapacityConflict struct{}
type errVersionConflict struct{}
type errMagicConflict struct{}
type errFormatConflict struct{}
type errBufferFull struct{}
type errBufferEmpty struct{}
type errRecordTooLarge struct{}
type errCorruptRecord struct{}
type errNotOpen struct{}

func (e errInvalidParam) Error() string     { return "ring: invalid parameter" }
func (e errCapacityConflict) Error() string { return "ring: capacity does not match existing buffer" }
func (e errVersionConflict) Error() string {
	return "ring: protocol version does not match existing buffer"
}
func (e errMagicConflict) Error() string {
	return "ring: magic mismatch: not a shared-memory ring buffer"
}
func (e errFormatConflict) Error() string { return "ring: format flags do not match existing buffer" }
func (e errBufferFull) Error() string     { return "ring: buffer full (no space for record)" }
func (e errBufferEmpty) Error() string    { return "ring: buffer empty (no committed record)" }
func (e errRecordTooLarge) Error() string { return "ring: record payload larger than capacity allows" }
func (e errCorruptRecord) Error() string  { return "ring: corrupt record (checksum mismatch)" }
func (e errNotOpen) Error() string        { return "ring: buffer is not open" }
