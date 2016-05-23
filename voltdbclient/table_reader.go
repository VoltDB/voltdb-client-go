package voltdbclient

import "bytes"

func readIntAt(r *bytes.Reader, off int64) (int32, error) {
	var b [4]byte
	bs := b[:4]
	_, err := r.ReadAt(bs, off)
	if err != nil {
		return 0, err
	}
	result := order.Uint32(bs)
	return int32(result), nil
}

func readStringAt(r *bytes.Reader, off int64) (string, error) {
	len, err := readIntAt(r, off)
	if err != nil {
		return "", err
	}
	if len == -1 {
		// NULL string not supported, return zero value
		return "", nil
	}
	bs := make([]byte, len)
	_, err = r.ReadAt(bs, off+4)
	if err != nil {
		return "", err
	}
	return string(bs), nil
}
