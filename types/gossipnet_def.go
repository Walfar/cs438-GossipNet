package types

import "crypto/rsa"

type FriendRequestMessage struct {
	PublicKey rsa.PublicKey
}

type PositiveResponse struct {
	PublicKey rsa.PublicKey
}

type NegativeResponse struct {
}

type EncryptedMessage struct {
	EncryptedBytes []byte
}
