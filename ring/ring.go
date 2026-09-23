package ring

import (
	"errors"
	"io/fs"
	"os"
	"sync"
	"sync/atomic"
	"unsafe"
)

// Ring is a process-private handle onto a named shared-memory ring buffer.
// Multiple processes may Create/Mount the same name; their capacity,
// format and version conventions must match.
type Ring struct {
	cfg  Config
	mf   *mappedFile
	wlk  *fileLock
	base []byte // header + data area
	data []byte // data area (cap bytes), offsets wrap here

	// In-process writer serialization. Cross-process serialization uses
	// the flock file (wlk); readers never take it.
	writeMu sync.Mutex
}

// RecoverInfo summarizes recovery work performed during Create/Mount.
type RecoverInfo struct {
	// Abandoned is 1 when a writer died after writing a record (or its
	// padding) but before publishing it (advancing writePos). Its bytes are
	// simply overwritten by the next writer; they were never committed.
	Abandoned int
	WritePos  uint64
	ReadPos   uint64
}

// Create creates a brand-new named buffer and mounts it. If a buffer with
// the same name already exists it fails wrapping fs.ErrExist and never
// touches the existing buffer.
func Create(cfg Config) (*Ring, RecoverInfo, error) {
	return open(cfg, true)
}

// Mount opens an existing named buffer and verifies that its capacity,
// format and protocol version match cfg. A mismatch fails with a
// distinguishable sentinel error and leaves the buffer untouched.
func Mount(cfg Config) (*Ring, RecoverInfo, error) {
	return open(cfg, false)
}

func open(cfg Config, create bool) (*Ring, RecoverInfo, error) {
	if err := cfg.validate(); err != nil {
		return nil, RecoverInfo{}, err
	}
	if err := os.MkdirAll(cfg.dir(), 0o755); err != nil {
		return nil, RecoverInfo{}, err
	}
	lock, err := newFileLock(cfg.lockPath())
	if err != nil {
		return nil, RecoverInfo{}, err
	}
	// Serializes creators/mounters/writers across processes.
	if err := lock.lock(); err != nil {
		lock.close()
		return nil, RecoverInfo{}, err
	}

	var mf *mappedFile
	if create {
		mf, err = createMappedFile(cfg.dataPath(), headerSize+cfg.Capacity)
	} else {
		mf, err = openMappedFile(cfg.dataPath())
	}
	if err != nil {
		_ = lock.unlock()
		_ = lock.close()
		return nil, RecoverInfo{}, err
	}

	r := &Ring{cfg: cfg, mf: mf, wlk: lock, base: mf.data, data: mf.data[headerSize:]}
	info := RecoverInfo{}

	if create {
		r.initHeader()
	} else {
		if err := r.verifyHeader(); err != nil {
			_ = mf.close()
			_ = lock.unlock()
			_ = lock.close()
			return nil, RecoverInfo{}, err
		}
		info = r.recoverAbandoned()
	}

	_ = lock.unlock()
	return r, info, nil
}

func (r *Ring) initHeader() {
	// Fill the data area with a non-zero erase pattern so that unwritten
	// slots never masquerade as a record whose start tag happens to equal
	// its logical position.
	for i := range r.data {
		r.data[i] = eraseFill
	}
	putU64(r.base, hdrMagicOff, ringMagic)
	putU64(r.base, hdrVerOff, Version)
	putU64(r.base, hdrCapOff, uint64(r.cfg.Capacity))
	putU64(r.base, hdrFmtOff, formatFlags)
	putU64(r.base, hdrWriteOff, 0)
	putU64(r.base, hdrReadOff, 0)
	putU64(r.base, hdrSeqOff, 0)
	putU64(r.base, hdrResvOff, 0)
	_ = r.mf.flush()
}

// verifyHeader checks every shared convention. It never writes, so a
// rejected mount cannot affect users of the buffer.
func (r *Ring) verifyHeader() error {
	if len(r.base) < headerSize {
		return ErrMagicConflict
	}
	if getU64(r.base, hdrMagicOff) != ringMagic {
		return ErrMagicConflict
	}
	if v := getU64(r.base, hdrVerOff); v != Version {
		return ErrVersionConflict
	}
	if c := getU64(r.base, hdrCapOff); c != uint64(r.cfg.Capacity) ||
		uint64(len(r.data)) != uint64(r.cfg.Capacity) {
		return ErrCapacityConflict
	}
	if f := getU64(r.base, hdrFmtOff); f != formatFlags {
		return ErrFormatConflict
	}
	return nil
}

// recoverAbandoned recognizes an unpublished record (or padding) left at
// writePos by a writer that crashed. Publication is the single atomic store
// advancing writePos, so a header whose start tag equals writePos was never
// visible; the next writer overwrites it. No position changes are needed.
func (r *Ring) recoverAbandoned() RecoverInfo {
	info := RecoverInfo{WritePos: r.writePos(), ReadPos: r.readPos()}
	w := advanceGap(r.writePos(), r.cfg.Capacity)
	hdr := r.readHdrAt(w)
	if recStart(hdr) == w {
		info.Abandoned = 1
	}
	return info
}

// writeRecord writes one record (recordTypeData or recordTypePad) without
// publishing; the caller advances writePos afterwards. It must hold wlk.
func (r *Ring) writeRecord(pos uint64, rtype int, payload []byte, seq uint64) error {
	hdr := make([]byte, recHdrSize)
	full := make([]byte, recHdrSize)
	encodeRecHdr(full, pos, len(payload), rtype, seq, 0)
	crc := calcCRC(full[:24], payload)
	encodeRecHdr(hdr, pos, len(payload), rtype, seq, crc)
	r.writeAt(hdr, pos)
	if len(payload) > 0 {
		r.writeAt(payload, pos+uint64(recHdrSize))
	}
	return nil
}

// Write appends one complete data record. Safe for concurrent writers
// across processes (flock) and goroutines (writeMu).
//
// The record becomes visible atomically: header, payload and checksum are
// written first (and flushed), then writePos advances in one aligned 8-byte
// atomic store. If the record would overwrite unconsumed data, Write fails
// with ErrBufferFull and writes nothing.
func (r *Ring) Write(payload []byte) (seq uint64, err error) {
	if r == nil || r.mf == nil {
		return 0, ErrNotOpen
	}
	if len(payload) > r.cfg.MaxPayload() {
		return 0, ErrRecordTooLarge
	}

	r.writeMu.Lock()
	defer r.writeMu.Unlock()
	if err := r.wlk.lock(); err != nil {
		return 0, err
	}
	defer r.wlk.unlock()

	w := r.writePos()
	rd := r.readPos()

	// Move past a trailing gap identically on both sides. A gap large
	// enough to hold a header is itself published as a pad record; a tiny
	// 8/16-byte gap is skipped with no bytes written.
	gap := gapSize(w, r.cfg.Capacity)
	wNext := w + uint64(gap)
	total := recordTotal(len(payload))
	end := wNext + uint64(total)
	// Never overwrite unconsumed records: used must stay <= capacity.
	if end-rd > uint64(r.cfg.Capacity) {
		return 0, ErrBufferFull
	}

	if gap >= recHdrSize {
		pad := make([]byte, gap-recHdrSize)
		if err := r.writeRecord(w, recordTypePad, pad, r.seq()); err != nil {
			return 0, err
		}
	}
	newSeq := r.seq() + 1
	if err := r.writeRecord(wNext, recordTypeData, payload, newSeq); err != nil {
		return 0, err
	}
	if err := r.mf.flush(); err != nil {
		return 0, err
	}

	putU64(r.base, hdrSeqOff, newSeq)
	atomic.StoreUint64(r.ptrAt(hdrWriteOff), end)
	return newSeq, nil
}

// head loads the committed head record. raw is the unmodified readPos;
// pos skips any trailing 8/16-byte naked gap with the exact rule writers
// use. A consumer acknowledges with one CAS from raw to the record end, so
// the naked gap is crossed atomically.
func (r *Ring) head() (raw, pos uint64, hdr []byte, ok bool) {
	w := atomic.LoadUint64(r.ptrAt(hdrWriteOff))
	raw = atomic.LoadUint64(r.ptrAt(hdrReadOff))
	pos = advanceGap(raw, r.cfg.Capacity)
	if pos >= w {
		return raw, pos, nil, false
	}
	hdr = r.readHdrAt(pos)
	if recStart(hdr) != pos {
		return raw, pos, hdr, false
	}
	return raw, pos, hdr, true
}

func (r *Ring) decodeRecord(pos uint64, hdr []byte) (seq uint64, payload []byte, next uint64, err error) {
	n := recLength(hdr)
	if n < 0 || recordTotal(n) > r.cfg.Capacity {
		return 0, nil, 0, ErrCorruptRecord
	}
	next = pos + uint64(recordTotal(n))
	if next > atomic.LoadUint64(r.ptrAt(hdrWriteOff)) {
		return 0, nil, 0, ErrCorruptRecord
	}
	payload = make([]byte, n)
	r.readAt(payload, pos+uint64(recHdrSize))
	if calcCRC(hdr[:24], payload) != recCRC(hdr) {
		return 0, nil, 0, ErrCorruptRecord
	}
	return recSeq(hdr), payload, next, nil
}

// Read consumes the next committed data record and returns its sequence
// number and payload. Concurrent readers each receive a distinct record
// exactly once (readPos advances via CAS). Trailing padding records are
// skipped automatically. After crash/re-mount reading resumes at the
// persisted readPos, so acknowledged data is never delivered again; use the
// sequence number (and Peek) to detect re-delivery before acknowledgment.
func (r *Ring) Read() (seq uint64, payload []byte, err error) {
	if r == nil || r.mf == nil {
		return 0, nil, ErrNotOpen
	}
	for {
		raw, pos, hdr, ok := r.head()
		if !ok {
			if hdr != nil {
				return 0, nil, ErrCorruptRecord
			}
			return 0, nil, ErrBufferEmpty
		}
		s, p, next, derr := r.decodeRecord(pos, hdr)
		if derr != nil {
			return 0, nil, derr
		}
		if recType(hdr) == recordTypePad {
			if !atomic.CompareAndSwapUint64(r.ptrAt(hdrReadOff), raw, next) {
				continue
			}
			continue
		}
		if !atomic.CompareAndSwapUint64(r.ptrAt(hdrReadOff), raw, next) {
			continue
		}
		return s, p, nil
	}
}

// Peek returns the next committed data record without advancing readPos.
// Repeated Peek calls return the same record and sequence number, allowing
// a consumer to recognize a re-delivered record rather than treat it as
// new. Commit performs the acknowledgment.
func (r *Ring) Peek() (seq uint64, payload []byte, err error) {
	if r == nil || r.mf == nil {
		return 0, nil, ErrNotOpen
	}
	for {
		raw, pos, hdr, ok := r.head()
		if !ok {
			if hdr != nil {
				return 0, nil, ErrCorruptRecord
			}
			return 0, nil, ErrBufferEmpty
		}
		s, p, _, derr := r.decodeRecord(pos, hdr)
		if derr != nil {
			return 0, nil, derr
		}
		if recType(hdr) == recordTypePad {
			next := pos + uint64(recordTotal(recLength(hdr)))
			if !atomic.CompareAndSwapUint64(r.ptrAt(hdrReadOff), raw, next) {
				continue
			}
			continue
		}
		return s, p, nil
	}
}

// Commit advances readPos past the current head record (padding included).
// It is the consumer acknowledgment point.
func (r *Ring) Commit() error {
	if r == nil || r.mf == nil {
		return ErrNotOpen
	}
	for {
		raw, pos, hdr, ok := r.head()
		if !ok {
			if hdr != nil {
				return ErrCorruptRecord
			}
			return ErrBufferEmpty
		}
		n := recLength(hdr)
		if n < 0 || recordTotal(n) > r.cfg.Capacity {
			return ErrCorruptRecord
		}
		next := pos + uint64(recordTotal(n))
		if next > atomic.LoadUint64(r.ptrAt(hdrWriteOff)) {
			return ErrCorruptRecord
		}
		if atomic.CompareAndSwapUint64(r.ptrAt(hdrReadOff), raw, next) {
			return nil
		}
	}
}

// Positions returns writePos, readPos and logical bytes used.
func (r *Ring) Positions() (writePos, readPos, used uint64) {
	w := atomic.LoadUint64(r.ptrAt(hdrWriteOff))
	rd := atomic.LoadUint64(r.ptrAt(hdrReadOff))
	return w, rd, w - rd
}

// LastSeq returns the sequence number of the newest published data record.
func (r *Ring) LastSeq() uint64 { return r.seq() }

func (r *Ring) Close() error {
	if r == nil || r.mf == nil {
		return nil
	}
	_ = r.mf.flush()
	err := r.mf.close()
	r.mf = nil
	if r.wlk != nil {
		_ = r.wlk.close()
		r.wlk = nil
	}
	return err
}

// ---- low level helpers ----

func (r *Ring) writePos() uint64 { return atomic.LoadUint64(r.ptrAt(hdrWriteOff)) }
func (r *Ring) readPos() uint64  { return atomic.LoadUint64(r.ptrAt(hdrReadOff)) }
func (r *Ring) seq() uint64      { return atomic.LoadUint64(r.ptrAt(hdrSeqOff)) }

func (r *Ring) ptrAt(off int) *uint64 {
	return (*uint64)(unsafe.Pointer(&r.base[off]))
}

func phys(pos uint64, cap int) int { return int(pos % uint64(cap)) }

// writeAt copies len(b) logical bytes at pos, wrapping at cap. Payloads are
// 8-byte padded, so every record header lands at an aligned offset.
func (r *Ring) writeAt(b []byte, pos uint64) {
	off := phys(pos, r.cfg.Capacity)
	n := copy(r.data[off:], b)
	if n < len(b) {
		copy(r.data, b[n:])
	}
}

func (r *Ring) readAt(dst []byte, pos uint64) {
	off := phys(pos, r.cfg.Capacity)
	n := copy(dst, r.data[off:])
	if n < len(dst) {
		copy(dst[n:], r.data)
	}
}

func (r *Ring) readHdrAt(pos uint64) []byte {
	hdr := make([]byte, recHdrSize)
	r.readAt(hdr, pos)
	return hdr
}

// gapSize returns the trailing gap at pos when the current lap cannot hold
// an empty 32-byte record header before the physical end. The gap is a
// positive multiple of 8 in [8, recHdrSize+alignMask]; zero means a record
// header fits. A gap of >= recHdrSize is filled with a published pad
// record; a gap of 8 or 16 bytes cannot hold a header and is skipped
// identically by writers and readers (it never contains a record).
func gapSize(pos uint64, cap int) int {
	off := phys(pos, cap)
	if off+recHdrSize <= cap {
		return 0
	}
	return cap - off
}

// advanceGap moves a logical position across any trailing gap. Both writer
// and reader use this, so gaps are interpreted identically on both sides.
func advanceGap(pos uint64, cap int) uint64 {
	return pos + uint64(gapSize(pos, cap))
}

func putU64(b []byte, off int, v uint64) {
	atomic.StoreUint64((*uint64)(unsafe.Pointer(&b[off])), v)
}

func getU64(b []byte, off int) uint64 {
	return atomic.LoadUint64((*uint64)(unsafe.Pointer(&b[off])))
}

// errIsExist reports whether err indicates an already-existing target.
func errIsExist(err error) bool { return errors.Is(err, fs.ErrExist) }
