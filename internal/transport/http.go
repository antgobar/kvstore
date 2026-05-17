package transport

type KeyValuePayload struct {
	Key   string `json:"key"`
	Value []byte `json:"value"`
}

type KeyPayload struct {
	Key string `json:"key"`
}
