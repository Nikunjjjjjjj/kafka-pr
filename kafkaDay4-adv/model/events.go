package model

type TrackEvent struct {
	UserID    int    `json:"userId"`
	Event     string `json:"event"`
	ProductID int    `json:"productId"`
}
