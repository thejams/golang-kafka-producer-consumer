package entity

// Message
type Message struct {
	MSG    string `json:"msg" validate:"required"`
	Sender string `json:"sender" validate:"required"`
}
