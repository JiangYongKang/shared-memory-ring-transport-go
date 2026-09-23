package ring

// Test-only wrappers around the public crash/recovery surface.

// crashWrite is retained as a short alias used by earlier tests.
func (r *Ring) crashWrite(payload []byte, flushBytes int) error {
	return r.CrashWrite(payload, flushBytes)
}

// tamperHeader overwrites one shared-header field directly in the mapped
// file, bypassing all validation. Test-only: it simulates a buffer created
// by an incompatible build/convention.
func tamperHeader(cfg Config, field int, value uint64) error {
	mf, err := openMappedFile(cfg.dataPath())
	if err != nil {
		return err
	}
	defer mf.close()
	putU64(mf.data, field, value)
	return mf.flush()
}

const (
	tamperMagic    = hdrMagicOff
	tamperVersion  = hdrVerOff
	tamperCapacity = hdrCapOff
	tamperFormat   = hdrFmtOff
)
