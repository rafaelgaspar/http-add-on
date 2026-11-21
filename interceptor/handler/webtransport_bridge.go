package handler

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"

	"github.com/quic-go/webtransport-go"
	"golang.org/x/sync/errgroup"

	"github.com/kedacore/http-add-on/pkg/util"
)

const (
	webTransportProtoH3    = "webtransport-h3"
	webTransportProtoDraft = "webtransport"
)

// WebTransportBridge terminates browser WebTransport on the interceptor and
// origins an equivalent session to QUIC-capable Xpra backends.
type WebTransportBridge struct {
	next        http.Handler
	wtServer    *webtransport.Server
	outboundTLS *tls.Config
}

func NewWebTransportBridge(next http.Handler, wtServer *webtransport.Server, outboundTLS *tls.Config) *WebTransportBridge {
	if next == nil || wtServer == nil {
		panic("WebTransportBridge requires next handler and webtransport.Server")
	}
	return &WebTransportBridge{
		next:        next,
		wtServer:    wtServer,
		outboundTLS: outboundTLS,
	}
}

var _ http.Handler = (*WebTransportBridge)(nil)

func (h *WebTransportBridge) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !isInboundWebTransport(r) {
		h.next.ServeHTTP(w, r)
		return
	}
	h.serveWebTransport(w, r)
}

func isInboundWebTransport(r *http.Request) bool {
	if r.Method != http.MethodConnect {
		return false
	}
	return r.Proto == webTransportProtoH3 || r.Proto == webTransportProtoDraft
}

func (h *WebTransportBridge) serveWebTransport(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	logger := util.LoggerFromContext(ctx).WithName("WebTransportBridge")

	url := util.UpstreamURLFromContext(ctx)
	if url == nil {
		NewStatic(http.StatusInternalServerError, errNilUpstreamURL).ServeHTTP(w, r)
		return
	}

	clientSess, err := h.wtServer.Upgrade(w, r)
	if err != nil {
		logger.Error(err, "webtransport upgrade failed")
		return
	}

	backendSess, err := h.dialBackendSession(ctx, r, url)
	if err != nil {
		logger.Error(err, "dialing backend webtransport session")
		_ = clientSess.CloseWithError(0, "")
		return
	}

	defer func() {
		_ = backendSess.CloseWithError(0, "")
		_ = clientSess.CloseWithError(0, "")
	}()

	if err := relayWebTransportSessions(ctx, clientSess, backendSess); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error(err, "webtransport relay")
	}
}

func (h *WebTransportBridge) dialBackendSession(ctx context.Context, r *http.Request, upstream *url.URL) (*webtransport.Session, error) {
	var tlsCfg *tls.Config
	if h.outboundTLS != nil {
		tlsCfg = h.outboundTLS.Clone()
	} else {
		tlsCfg = &tls.Config{}
	}
	if len(tlsCfg.NextProtos) == 0 {
		tlsCfg.NextProtos = []string{"h3"}
	}
	serverName := util.UpstreamServerNameFromContext(ctx)
	if serverName != "" {
		tlsCfg.ServerName = serverName
	}

	backendURL := *upstream
	if backendURL.Scheme == "" || backendURL.Scheme == "http" {
		backendURL.Scheme = "https"
	}
	backendURL.Path = singleJoin(backendURL.Path, r.URL.Path)
	if r.URL.RawQuery != "" {
		backendURL.RawQuery = r.URL.RawQuery
	}

	hdr := cloneForwardHeaders(r.Header)
	tr := &webtransport.Transport{TLSClientConfig: tlsCfg}
	_, sess, err := tr.Dial(ctx, backendURL.String(), hdr)
	if err != nil {
		return nil, fmt.Errorf("webtransport dial %s: %w", backendURL.String(), err)
	}
	return sess, nil
}

func relayWebTransportSessions(ctx context.Context, a, b *webtransport.Session) error {
	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error { return pumpBidiStreams(ctx, a, b) })
	g.Go(func() error { return pumpBidiStreams(ctx, b, a) })

	return g.Wait()
}

func pumpBidiStreams(ctx context.Context, src, dst *webtransport.Session) error {
	for {
		in, err := src.AcceptStream(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		out, err := dst.OpenStreamSync(ctx)
		if err != nil {
			_ = in.Close()
			return err
		}
		go func(in, out *webtransport.Stream) {
			var wg sync.WaitGroup
			wg.Add(2)
			go func() { defer wg.Done(); _, _ = io.Copy(out, in) }()
			go func() { defer wg.Done(); _, _ = io.Copy(in, out) }()
			wg.Wait()
			_ = in.Close()
			_ = out.Close()
		}(in, out)
	}
}

func cloneForwardHeaders(hdr http.Header) http.Header {
	out := make(http.Header, len(hdr))
	for k, vv := range hdr {
		if k == "Host" {
			continue
		}
		out[k] = append([]string(nil), vv...)
	}
	return out
}

func singleJoin(a, b string) string {
	switch {
	case a == "" || a == "/":
		return b
	case b == "":
		return a
	default:
		return a + b
	}
}