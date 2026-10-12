// Package server assembles HTTP routes and handlers.
package server

import (
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/ravenmk2/mayfly/web"
)

// APIRouter registers the management REST API routes; internal/admin
// implements it. Declared here so the HTTP skeleton stays decoupled from the
// admin package.
type APIRouter interface {
	// RegisterRoutes mounts the API routes; they must precede the SPA
	// fallback.
	RegisterRoutes(e *echo.Echo)
}

// Options carries the admin HTTP server dependencies.
type Options struct {
	// MetricsHandler serves GET /metrics (Prometheus exposition); nil
	// disables the endpoint.
	MetricsHandler http.Handler
	// APIRouter mounts the management REST API under /api/v1; nil disables
	// the API.
	APIRouter APIRouter
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
	if opts.APIRouter != nil {
		opts.APIRouter.RegisterRoutes(e)
	}

	// SPA fallback: wildcard route falls back to index.html for the frontend router
	e.GET("/*", echo.WrapHandler(SPAHandler(web.DistFS())))
	return e
}
