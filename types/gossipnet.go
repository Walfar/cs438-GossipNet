package types

import (
	"fmt"
)

// -----------------------------------------------------------------------------
func (fr FriendRequestMessage) NewEmpty() Message {
	return &FriendRequestMessage{}
}

func (FriendRequestMessage) Name() string {
	return "friendRequest"
}

func (fr FriendRequestMessage) String() string {
	return fmt.Sprintf("<%s>", "friend request")
}

func (fr FriendRequestMessage) HTML() string {
	return "friend request"
}

//================================================================

func (pr PositiveResponse) NewEmpty() Message {
	return &PositiveResponse{}
}

func (PositiveResponse) Name() string {
	return "positiveResponse"
}

func (pr PositiveResponse) String() string {
	return fmt.Sprintf("<%s>", "positive response")
}

func (pr PositiveResponse) HTML() string {
	return "positive response"
}

//================================================================

func (nr NegativeResponse) NewEmpty() Message {
	return &NegativeResponse{}
}

func (NegativeResponse) Name() string {
	return "negativeResponse"
}

func (nr NegativeResponse) String() string {
	return fmt.Sprintf("<%s>", "negative response")
}

func (nr NegativeResponse) HTML() string {
	return "negative response"
}

//================================================================

func (em EncryptedMessage) NewEmpty() Message {
	return &EncryptedMessage{}
}

func (em EncryptedMessage) Name() string {
	return "encryptedMessage"
}

func (em EncryptedMessage) String() string {
	return fmt.Sprintf("<%s>", "encrypted message")
}

func (em EncryptedMessage) HTML() string {
	return "encrypted message"
}
