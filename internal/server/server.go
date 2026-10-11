// Package server assembles HTTP routes and handlers.
package server

import (
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/ravenmk2/mayfly/web"
)

// Options carries the admin HTTP server dependencies.
type Options struct {
	// MetricsHandler serves GET /metrics (Prometheus exposition); nil
	// disables the endpoint.
	MetricsHandler http.Handler
}

// New assembles routes and returns the root HTTP handler.
func New(opts Options) http.Handler {
	e := echo.New()
	e.GET("/healthz", func(c echo.Context) error {
		return c.String(http.StatusOK, "ok")
	})
	if opts.MetricsHandler != nil {
		e.GET("/metrics", echo.WrapHandler(opts.MetricsHandler))
	}

	// SPA fallback: wildcard route falls back to index.html for the frontend router
	e.GET("/*", echo.WrapHandler(SPAHandler(web.DistFS())))
	return e
}
