package ring

import (
	"encoding/binary"
	"hash/crc32"
	"path/filepath"
	"strings"
)

// On-disk / shared-memory layout.
//
// The mapping file starts with a fixed 64-byte shared header, followed by
// the ring data area. Every multi-byte field is 8-byte aligned so it can
// be accessed atomically across processes. All integers are little-endian.
//
//	offset  size  field
//	0       8     magic   ("SMRTRNG1")
//	8       8     version (protocol/version convention)
//	16      8     capacity (usable data-area size in bytes, multiple of 8)
//	24      8     format  (format flags, e.g. checksum algorithm)
//	32      8     writePos (monotonic logical position; commit point)
//	40      8     readPos  (monotonic logical position; consumer ack point)
//	48      8     writeSeq (number of data records ever published)
//	56      8     reserved (0)
const (
	hdrMagicOff = 0
	hdrVerOff   = 8
	hdrCapOff   = 16
	hdrFmtOff   = 24
	hdrWriteOff = 32
	hdrReadOff  = 40
	hdrSeqOff   = 48
	hdrResvOff  = 56

	headerSize = 64
)

// ringMagic identifies a shared-memory ring buffer mapping file.
var ringMagic = func() uint64 {
	var b [8]byte
	copy(b[:], "SMRTRNG1")
	return binary.LittleEndian.Uint64(b[:])
}()

// Version is the protocol version implemented by this package. Mounting a
// buffer created by another version is rejected (ErrVersionConflict).
const Version uint64 = 1

// Format flags agreed upon by every process mounting one buffer.
const (
	fmtCRC32IEEE uint64 = 1 << 0
	formatFlags         = fmtCRC32IEEE
)

// Record header: fixed 32 bytes, 8-byte aligned, never split across the
// physical end of the ring. A record's payload directly follows the header
// and is padded up to an 8-byte boundary, so every record start stays
// 8-byte aligned.
//
//	offset  size  field
//	0       8     start  (logical position of this record; generation tag)
//	8       4     length (payload length in bytes)
//	12      4     rtype  (recordTypeData / recordTypePad)
//	16      8     seq    (monotonic data-record sequence number)
//	24      4     crc    (CRC-32/IEEE over header[0:24] ++ payload)
//	28      4     reserved (0)
const (
	recStartOff = 0
	recLenOff   = 8
	recTypeOff  = 12
	recSeqOff   = 16
	recCrcOff   = 24

	recHdrSize = 32
)

const (
	recordTypeData = 0
	recordTypePad  = 1
)

const (
	alignMask    = 7
	minCapacity  = 1024
	maxNameLen   = 256
	dataFileExt  = ".ring"
	lockFileExt  = ".wlock"
	defaultDirNm = "smrt-ring"

	// eraseFill is written to a fresh data area. It is deliberately
	// non-zero: an "unwritten" slot then reads as an obviously invalid
	// record start tag (never equal to a logical position, which can be 0
	// only for the very first record and whose header is then written by
	// Create with its real start tag).
	eraseFill byte = 0xEE
)

func align8(n int) int { return (n + alignMask) &^ alignMask }

// Config carries the conventions every process mounting one named buffer
// must agree on. Capacity is required even for Mount: it must match the
// existing buffer or the mount is rejected.
type Config struct {
	// Name identifies the buffer within Dir. Must be non-empty.
	Name string
	// Dir holds the mapping/lock files. Empty means the OS temp directory
	// under "smrt-ring".
	Dir string
	// Capacity is the usable ring size in bytes. Must be a multiple of 8
	// and >= 1024.
	Capacity int
}

func (c Config) validate() error {
	if strings.TrimSpace(c.Name) == "" || len(c.Name) > maxNameLen {
		return ErrInvalidParam
	}
	if strings.ContainsAny(c.Name, `/\`) {
		return ErrInvalidParam
	}
	if c.Capacity < minCapacity || c.Capacity&alignMask != 0 {
		return ErrInvalidParam
	}
	return nil
}

func (c Config) dir() string {
	if c.Dir != "" {
		return c.Dir
	}
	return filepath.Join(tempDir(), defaultDirNm)
}

func (c Config) dataPath() string { return filepath.Join(c.dir(), c.Name+dataFileExt) }
func (c Config) lockPath() string { return filepath.Join(c.dir(), c.Name+lockFileExt) }

// MaxPayload returns the largest payload that fits this capacity. A record
// occupies recHdrSize + align8(payload) logical bytes and must fit in one
// lap of the ring.
func (c Config) MaxPayload() int { return c.Capacity - recHdrSize }

// recordLen is the total logical size of a record with payload length n.
func recordTotal(n int) int { return recHdrSize + align8(n) }

func encodeRecHdr(buf []byte, start uint64, length, rtype int, seq uint64, crc uint32) {
	binary.LittleEndian.PutUint64(buf[recStartOff:], start)
	binary.LittleEndian.PutUint32(buf[recLenOff:], uint32(length))
	binary.LittleEndian.PutUint32(buf[recTypeOff:], uint32(rtype))
	binary.LittleEndian.PutUint64(buf[recSeqOff:], seq)
	binary.LittleEndian.PutUint32(buf[recCrcOff:], crc)
	binary.LittleEndian.PutUint32(buf[28:], 0)
}

func recStart(buf []byte) uint64 { return binary.LittleEndian.Uint64(buf[recStartOff:]) }
func recLength(buf []byte) int   { return int(binary.LittleEndian.Uint32(buf[recLenOff:])) }
func recType(buf []byte) uint32  { return binary.LittleEndian.Uint32(buf[recTypeOff:]) }
func recSeq(buf []byte) uint64   { return binary.LittleEndian.Uint64(buf[recSeqOff:]) }
func recCRC(buf []byte) uint32   { return binary.LittleEndian.Uint32(buf[recCrcOff:]) }

// calcCRC computes the record checksum over the first 24 header bytes
// (everything except the checksum/reserved words) and the raw payload.
func calcCRC(hdr24 []byte, payload []byte) uint32 {
	h := crc32.NewIEEE()
	h.Write(hdr24)
	h.Write(payload)
	return h.Sum32()
}
