package ring

// CrashWrite models a writer process dying mid-write: it writes exactly
// flushBytes of the would-be record's logical byte stream (trailing pad, if
// any, then the 32-byte header, then the payload) and never advances
// writePos. It also drops the cross-process writer lock exactly the way the
// kernel would when a process dies (closing the lock fd).
//
// The Ring handle must not be used for further writes afterwards; close it
// and Mount again to observe recovery. This is intentionally part of the
// public API so crash recovery is reproducible from real separate
// processes (see cmd/smrt crash) rather than only in tests.
//
// flushBytes may exceed the logical length, in which case the full record
// is left on disk but still unpublished.
func (r *Ring) CrashWrite(payload []byte, flushBytes int) error {
	if r == nil || r.mf == nil {
		return ErrNotOpen
	}
	if len(payload) > r.cfg.MaxPayload() {
		return ErrRecordTooLarge
	}
	r.writeMu.Lock()
	if err := r.wlk.lock(); err != nil {
		r.writeMu.Unlock()
		return err
	}
	// Simulate process death releasing the flock: close the lock fd
	// without flock(UNLOCK). The kernel releases the lock automatically.
	_ = r.wlk.f.Close()
	r.wlk.taken = false
	r.wlk.f = nil
	r.writeMu.Unlock()

	w := r.writePos()
	gap := gapSize(w, r.cfg.Capacity)
	wNext := advanceGap(w, r.cfg.Capacity)
	newSeq := r.seq() + 1

	logical := make([]byte, 0, gap+recHdrSize+len(payload))
	if gap >= recHdrSize {
		pad := make([]byte, gap-recHdrSize)
		pf := make([]byte, recHdrSize)
		encodeRecHdr(pf, w, len(pad), recordTypePad, r.seq(), 0)
		pcrc := calcCRC(pf[:24], pad)
		ph := make([]byte, recHdrSize)
		encodeRecHdr(ph, w, len(pad), recordTypePad, r.seq(), pcrc)
		logical = append(logical, ph...)
		logical = append(logical, pad...)
	}
	hf := make([]byte, recHdrSize)
	encodeRecHdr(hf, wNext, len(payload), recordTypeData, newSeq, 0)
	crc := calcCRC(hf[:24], payload)
	hh := make([]byte, recHdrSize)
	encodeRecHdr(hh, wNext, len(payload), recordTypeData, newSeq, crc)
	logical = append(logical, hh...)
	logical = append(logical, payload...)

	if flushBytes > len(logical) {
		flushBytes = len(logical)
	}
	if flushBytes < 0 {
		flushBytes = 0
	}
	r.writeAt(logical[:flushBytes], w)
	_ = r.mf.flush()
	r.mf = nil // handle unusable for further writes (simulated death)
	return nil
}
