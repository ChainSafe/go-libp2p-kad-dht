package internal

import (
	"fmt"
	"testing"

	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

func TestPrefixByBits(t *testing.T) {
	testCases := []struct {
		key      []byte
		bits     int
		expected []byte
	}{
		{
			key:      []byte{0xff, 0xff},
			bits:     0,
			expected: []byte{0xff, 0xff},
		},
		{
			key:      []byte{0xff, 0xff},
			bits:     1,
			expected: []byte{0x1},
		},
		{
			key:      []byte{0xff, 0xff},
			bits:     2,
			expected: []byte{0x3},
		},
		{
			key:      []byte{0xff, 0xff},
			bits:     3,
			expected: []byte{0x7},
		},
		{
			key:      []byte{0xff, 0xff},
			bits:     4,
			expected: []byte{0b00001111},
		},
		{
			key:      []byte{0xff, 0xff},
			bits:     5,
			expected: []byte{0b00011111},
		},
		{
			key:      []byte{0xff, 0xff},
			bits:     6,
			expected: []byte{0b00111111},
		},
		{
			key:      []byte{0xff, 0xff},
			bits:     7,
			expected: []byte{0b01111111},
		},
		{
			key:      []byte{0xff, 0xff},
			bits:     8,
			expected: []byte{0xff},
		},
		{
			key:      []byte{0xff, 0xff},
			bits:     9,
			expected: []byte{0xff, 0x1},
		},
		{
			key:      []byte{0xff, 0xff},
			bits:     10,
			expected: []byte{0xff, 0x3},
		},
		{
			key:      []byte{0xff, 0xff},
			bits:     11,
			expected: []byte{0xff, 0x7},
		},
		{
			key:      []byte{0xff, 0xff},
			bits:     12,
			expected: []byte{0xff, 0b00001111},
		},
		{
			key:      []byte{0xff, 0xff},
			bits:     13,
			expected: []byte{0xff, 0b00011111},
		},
		{
			key:      []byte{0xff, 0xff},
			bits:     14,
			expected: []byte{0xff, 0b00111111},
		},
		{
			key:      []byte{0xff, 0xff},
			bits:     15,
			expected: []byte{0xff, 0b01111111},
		},
		{
			key:      []byte{0xff, 0xff},
			bits:     16,
			expected: []byte{0xff, 0xff},
		},
		{
			key:      []byte{0xff, 0xff},
			bits:     17,
			expected: []byte{0xff, 0xff},
		},
	}

	for i, tc := range testCases {
		res := PrefixByBits(tc.key, tc.bits)
		require.Equal(t, tc.expected, res, fmt.Sprintf("case %d failed", i))
	}
}

func TestDecodePrefixedKey(t *testing.T) {
	mh, _ := Sha256Multihash([]byte("nootwashere"))
	require.Equal(t, 34, len(mh))
	decodedMH, err := multihash.Decode(mh)
	require.NoError(t, err)

	slicedMH := mh[:17]
	slicedDigest, err := DecodePrefixedKey(slicedMH)
	require.NoError(t, err)
	require.Equal(t, decodedMH.Digest[:15], slicedDigest)
}
