package dht

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"errors"

	"github.com/multiformats/go-multihash"
)

const (
	keySize               = 32
	encryptedPeerIDLength = 66
)

var errInvalidKeySize = errors.New("key size must be 32 bytes")

func encryptAES(plaintext, key []byte) ([]byte, error) {
	aesgcm, err := newAESGCM(key)
	if err != nil {
		return nil, err
	}

	nonce := make([]byte, aesgcm.NonceSize())
	_, err = rand.Read(nonce)
	if err != nil {
		return nil, err
	}

	ct := aesgcm.Seal(nil, nonce, plaintext, nil)
	return append(nonce, ct...), nil
}

func decryptAES(nonceAndCT, key []byte) ([]byte, error) {
	aesgcm, err := newAESGCM(key)
	if err != nil {
		return nil, err
	}

	nonce := nonceAndCT[:aesgcm.NonceSize()]
	ciphertext := nonceAndCT[aesgcm.NonceSize():]
	plaintext, err := aesgcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, err
	}

	return plaintext, nil
}

func newAESGCM(key []byte) (cipher.AEAD, error) {
	if len(key) != keySize {
		return nil, errInvalidKeySize
	}

	block, err := aes.NewCipher(key[:])
	if err != nil {
		return nil, err
	}

	aesgcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	return aesgcm, nil
}

func multihashToKey(mh multihash.Multihash) []byte {
	const prefix = "AESGCM"
	h := sha256.Sum256(append([]byte(prefix), mh...))
	return h[:]
}
