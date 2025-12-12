package worker

type Event struct {
	UserID    int    `json:"userId"`
	Event     string `json:"event"`
	ProductID int    `json:"productId"`
}
