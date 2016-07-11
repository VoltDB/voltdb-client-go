package voltdbclient

import (
	"bytes"
	"database/sql/driver"
	"encoding/binary"
	"encoding/json"
	"strconv"

	"errors"
	"github.com/spaolacci/murmur3"
	"log"
	"sort"
)

// Hash Type
const (
	LEGACY  = "LEGACY"
	ELASTIC = "ELASTIC"
)

// Hash Config Format
const (
	BINARAY_FORMAT = 0
	JSON_FORMAT    = 1
)

type hashinater interface {
	getConfigurationType() string

	/**
	 * Given the type of the targeting partition parameter and an object,
	 * coerce the object to the correct type and hash it.
	 */
	getHashedPartitionForParameter(partitionParameterType int, partitionValue driver.Value) (hashedPartition int, err error)
}

type hashinaterElastic struct {
	// sorted array store token index
	a []int
	// unsorted map store token to partition
	m map[int]int
}

func newHashinaterElasticEmpty() *hashinaterElastic {
	h := new(hashinaterElastic)
	return h
}

func newHashinaterElastic(hashConfigFormat int, cooked bool, hashConfig []byte) (h *hashinaterElastic, err error) {
	if hashConfigFormat != JSON_FORMAT {
		return nil, errors.New("Not support non JSON hash config.")
	}
	h = new(hashinaterElastic)
	if cooked {
		hashConfig, err = fromGzip(hashConfig)
		if err != nil {
			return nil, err
		}
	}

	// unmarshall json
	if err = h.unmarshalJSONConfig(hashConfig); err != nil {
		return nil, err
	}
	log.Println(h.a)
	log.Println(h.m)
	return h, nil
}

func (h hashinaterElastic) getConfigurationType() string {
	return ELASTIC
}

func (h hashinaterElastic) getHashedPartitionForParameter(partitionParameterType int, partitionValue driver.Value) (hashedPartition int, err error) {

	// TODO Handle Special cases:
	// 1) if the user supplied a string for a number column,
	// try to do the conversion.
	return h.hashinateBytes(valueToBytes(partitionValue))
}

/**
 * Given an byte[] bytes, pick a partition to store the data.
 */
func (h hashinaterElastic) hashinateBytes(b []byte) (partition int, err error) {
	if b == nil {
		return 0, nil
	}

	v1, _ := murmur3.Sum128(b)

	//Shift so that we use the higher order bits in case we want to use the lower order ones later
	//Also use the h1 higher order bits because it provided much better performance in voter, consistent too
	hash := int(v1 >> 32) // golang do logic shift on unsigned integer
	token := sort.SearchInts(h.a, hash)
	return h.m[token], nil
}

// TODO move this function to proper place (volttype, voltserializer ?)
func valueToBytes(v driver.Value) []byte {
	if v == nil {
		return nil
	}
	switch v.(type) {
	case nullValue:
		return nil
	case []byte:
		return v.([]byte)
	case string:
		return []byte(v.(string))
	default: // should exclude other none int type ?
		buf := new(bytes.Buffer)
		err := binary.Write(buf, binary.LittleEndian, v)
		if err != nil {
			panicIfnotNil("binary.Write failed:", err)
		}
		return buf.Bytes()
	}
}

// until go 1.7, go lang won't support non-string type keys for (un-)marshal
// https://github.com/golang/go/commit/ffbd31e9f79ad8b6aaeceac1397678e237581064
// need to one more loop for the conversation
func (h *hashinaterElastic) unmarshalJSONConfig(bytes []byte) (err error) {

	// Unmarshal the string-keyed map
	sk := make(map[string]int)
	err = json.Unmarshal(bytes, &sk)
	if err != nil {
		return
	}

	h.a = make([]int, 0, 8)
	h.m = make(map[int]int)
	// Copy the values

	var ki int
	for k, v := range sk {
		ki, err = strconv.Atoi(k)
		if err != nil {
			return
		}
		h.m[ki] = v
		h.a = append(h.a, ki)
	}
	sort.Ints(h.a)

	return
}
