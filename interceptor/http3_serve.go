package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"sync/atomic"

	"github.com/go-logr/logr"
	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"github.com/quic-go/webtransport-go"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/kedacore/http-add-on/interceptor/config"
	"github.com/kedacore/http-add-on/interceptor/metrics"
	"github.com/kedacore/http-add-on/pkg/k8s"
	"github.com/kedacore/http-add-on/pkg/queue"
	"github.com/kedacore/http-add-on/pkg/routing"
	"github.com/kedacore/http-add-on/pkg/util"
)

func runHTTP3ProxyServer(
	ctx context.Context,
	logger logr.Logger,
	q queue.Counter,
	readyCache *k8s.ReadyEndpointsCache,
	routingTable routing.Table,
	reader client.Reader,
	timeouts config.Timeouts,
	serving config.Serving,
	port int,
	servingTLS *tls.Config,
	outboundTLS *tls.Config,
	tracingConfig config.Tracing,
	instruments *metrics.Instruments,
	_ *atomic.Bool,
) error {
	addr := fmt.Sprintf("0.0.0.0:%d", port)
	logger = logger.WithName("runHTTP3ProxyServer")

	tlsCfg := http3.ConfigureTLSConfig(servingTLS.Clone())
	h3 := &http3.Server{
		Addr:      addr,
		TLSConfig: tlsCfg,
		QUICConfig: &quic.Config{
			EnableDatagrams:                  true,
			EnableStreamResetPartialDelivery: true,
		},
	}
	wtSrv := &webtransport.Server{
		H3: h3,
		CheckOrigin: func(*http.Request) bool {
			return true
		},
	}

	rootHandler := BuildProxyHandler(&ProxyHandlerConfig{
		Logger:             logger,
		Queue:              q,
		ReadyCache:         readyCache,
		RoutingTable:       routingTable,
		Reader:             reader,
		Timeouts:           timeouts,
		Serving:            serving,
		ServingTLS:         servingTLS,
		OutboundTLS:        outboundTLS,
		Tracing:            tracingConfig,
		Instruments:        instruments,
		WebTransportServer: wtSrv,
		HTTP3ProxyEnabled:  true,
	})
	h3.Handler = rootHandler

	errCh := make(chan error, 1)
	go func() {
		logger.Info("HTTP/3 proxy server starting", "address", addr)
		errCh <- wtSrv.ListenAndServe()
	}()

	select {
	case <-ctx.Done():
		logger.Info("HTTP/3 proxy server shutting down")
		if err := wtSrv.Close(); err != nil {
			logger.Error(err, "error closing HTTP/3 server")
		}
		return nil
	case err := <-errCh:
		if util.IsIgnoredErr(err) {
			return nil
		}
		return err
	}
}