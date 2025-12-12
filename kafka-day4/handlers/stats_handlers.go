package handlers

import (
	"database/sql"
	"kafka-day4/database"
	"net/http"

	"github.com/gin-gonic/gin"
)

func GetProductStats(c *gin.Context) {
	id := c.Param("id")

	var views int64
	err := database.DB.QueryRow("SELECT views FROM product_stats WHERE product_id = $1", id).Scan(&views)

	if err == sql.ErrNoRows {
		c.JSON(http.StatusOK, gin.H{
			"productId": id,
			"views":     0,
		})
		return
	}

	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"productId": id,
		"views":     views,
	})
}
