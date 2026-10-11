// Package server assembles HTTP routes and handlers.
package server

import (
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/ravenmk2/mayfly/web"
)

// New assembles routes and returns the root HTTP handler.
func New() http.Handler {
	e := echo.New()
	e.GET("/healthz", func(c echo.Context) error {
		return c.String(http.StatusOK, "ok")
	})

	// SPA fallback: wildcard route falls back to index.html for the frontend router
	e.GET("/*", echo.WrapHandler(SPAHandler(web.DistFS())))
	return e
}
