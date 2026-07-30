package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/dns/dnsmessage"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

type countingReadCloser struct {
	reader io.Reader
	read   int64
	closed bool
}

func (r *countingReadCloser) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	r.read += int64(n)
	return n, err
}

func (r *countingReadCloser) Close() error {
	r.closed = true
	return nil
}

func TestCandidateOriginPolicy(t *testing.T) {
	valid := []string{
		"https://example.com",
		"https://example.com/",
		"https://example.com:443",
		"https://8.8.8.8",
		"https://[2606:4700:4700::1111]",
	}
	for _, rawURL := range valid {
		t.Run("valid_"+rawURL, func(t *testing.T) {
			require.NoError(t, validateCandidateOrigin(rawURL))
		})
	}

	invalid := []string{
		"http://example.com",
		"//example.com",
		"https://user:password@example.com",
		"https://example.com/path",
		"https://example.com?query=1",
		"https://example.com?",
		"https://example.com#fragment",
		"https://example.com:8443",
		"https://example.com:",
		"https://localhost",
		"https://service.localhost",
		"https://127.0.0.1",
		"https://10.0.0.1",
		"https://100.64.0.1",
		"https://169.254.1.1",
		"https://192.0.2.1",
		"https://198.51.100.1",
		"https://203.0.113.1",
		"https://224.0.0.1",
		"https://[::1]",
		"https://[fe80::1]",
		"https://[fc00::1]",
		"https://[2001:db8::1]",
		"https://[3fff::1]",
		"https://[2001:2::1]",
		"https://[2001:10::1]",
		"https://[2001:20::1]",
		"https://[2002::1]",
		"https://[ff02::1]",
		"https://[::ffff:10.0.0.1]",
	}
	for _, rawURL := range invalid {
		t.Run("invalid_"+rawURL, func(t *testing.T) {
			require.Error(t, validateCandidateOrigin(rawURL))
		})
	}
}

func TestCandidateConnectedAddressPolicy(t *testing.T) {
	tests := []struct {
		address string
		wantErr bool
	}{
		{address: "8.8.8.8:443"},
		{address: "[2606:4700:4700::1111]:443"},
		{address: "127.0.0.1:443", wantErr: true},
		{address: "10.0.0.1:443", wantErr: true},
		{address: "100.64.0.1:443", wantErr: true},
		{address: "169.254.1.1:443", wantErr: true},
		{address: "192.0.2.1:443", wantErr: true},
		{address: "224.0.0.1:443", wantErr: true},
		{address: "[2001:db8::1]:443", wantErr: true},
		{address: "[3fff::1]:443", wantErr: true},
		{address: "[2001:2::1]:443", wantErr: true},
		{address: "[2001:10::1]:443", wantErr: true},
		{address: "[2001:20::1]:443", wantErr: true},
		{address: "[2002::1]:443", wantErr: true},
		{address: "[ff02::1]:443", wantErr: true},
		{address: "[::ffff:10.0.0.1]:443", wantErr: true},
	}
	for _, tt := range tests {
		network := "tcp4"
		if strings.HasPrefix(tt.address, "[") {
			network = "tcp6"
		}
		err := candidatePublicOnlyControl(network, tt.address, nil)
		if tt.wantErr {
			require.Error(t, err, tt.address)
		} else {
			require.NoError(t, err, tt.address)
		}
	}
}

func TestCandidateOriginCanonicalizesSoleSlash(t *testing.T) {
	origin, err := canonicalCandidateOrigin("https://plc.example/")
	require.NoError(t, err)
	assert.Equal(t, "https://plc.example", origin)

	did := syntax.DID("did:plc:wqgdnqlv2mwiio6pfchwtrff")
	client := newCandidateHTTPClientWithTransport(time.Second, 1024, roundTripFunc(func(req *http.Request) (*http.Response, error) {
		assert.Equal(t, "/"+did.String(), req.URL.Path)
		assert.NotEqual(t, "//"+did.String(), req.URL.Path)
		body := `{"id":"` + did.String() + `"}`
		return &http.Response{StatusCode: http.StatusOK, ContentLength: int64(len(body)), Body: io.NopCloser(strings.NewReader(body)), Request: req}, nil
	}))
	directory := identity.BaseDirectory{PLCURL: origin, HTTPClient: *client}
	_, err = directory.ResolveDID(t.Context(), did)
	require.NoError(t, err)
}

func TestCandidateTransportDoesNotDecompressResponses(t *testing.T) {
	payload := bytes.Repeat([]byte("amplified"), 1024)
	var compressed bytes.Buffer
	zipper := gzip.NewWriter(&compressed)
	_, err := zipper.Write(payload)
	require.NoError(t, err)
	require.NoError(t, zipper.Close())

	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		assert.Empty(t, req.Header.Get("Accept-Encoding"))
		w.Header().Set("Content-Encoding", "gzip")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(compressed.Bytes())
	}))
	defer server.Close()
	transport := server.Client().Transport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.DisableCompression = true
	transport.DialContext = func(ctx context.Context, network, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, server.Listener.Addr().String())
	}
	client := newCandidateHTTPClientWithTransport(time.Second, int64(compressed.Len()+1), transport)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://example.com/data", nil)
	require.NoError(t, err)
	resp, err := client.Do(req)
	require.NoError(t, err)
	data, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.NoError(t, err)
	assert.Equal(t, "gzip", resp.Header.Get("Content-Encoding"))
	assert.Equal(t, compressed.Bytes(), data)
	assert.NotEqual(t, payload, data)
}

func TestCandidateDialerRejectsDNSRebindingToPrivate(t *testing.T) {
	dnsAddr, stop := startPrivateDNS(t)
	defer stop()
	resolver := &net.Resolver{
		PreferGo: true,
		Dial: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "udp", dnsAddr)
		},
	}
	dialer := newCandidatePublicOnlyDialer(resolver)
	_, err := dialer.DialContext(t.Context(), "tcp", "rebinding.test:443")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not a public IP address")
}

func startPrivateDNS(t *testing.T) (string, func()) {
	t.Helper()
	conn, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	done := make(chan struct{})
	go func() {
		defer close(done)
		buffer := make([]byte, 512)
		for {
			n, addr, err := conn.ReadFrom(buffer)
			if err != nil {
				return
			}
			var parser dnsmessage.Parser
			header, err := parser.Start(buffer[:n])
			if err != nil {
				continue
			}
			question, err := parser.Question()
			if err != nil {
				continue
			}
			builder := dnsmessage.NewBuilder(nil, dnsmessage.Header{ID: header.ID, Response: true, RecursionAvailable: true})
			_ = builder.StartQuestions()
			_ = builder.Question(question)
			_ = builder.StartAnswers()
			_ = builder.AResource(dnsmessage.ResourceHeader{Name: question.Name, Type: dnsmessage.TypeA, Class: dnsmessage.ClassINET, TTL: 1}, dnsmessage.AResource{A: [4]byte{127, 0, 0, 1}})
			response, err := builder.Finish()
			if err == nil {
				_, _ = conn.WriteTo(response, addr)
			}
		}
	}()
	return conn.LocalAddr().String(), func() {
		_ = conn.Close()
		<-done
	}
}

func TestCandidateTransportDisablesProxyAndCompression(t *testing.T) {
	client := newCandidateHTTPClient(time.Second, 1024)
	policyTransport := client.Transport.(*candidateRoundTripper)
	transport := policyTransport.base.(*http.Transport)

	assert.Nil(t, transport.Proxy)
	assert.True(t, transport.DisableCompression)
	_, err := transport.DialContext(t.Context(), "tcp", "8.8.8.8:80")
	require.Error(t, err)
}

func TestCandidateTransportRejectsURLBeforeRoundTrip(t *testing.T) {
	var calls atomic.Int32
	client := newCandidateHTTPClientWithTransport(time.Second, 1024, roundTripFunc(func(req *http.Request) (*http.Response, error) {
		calls.Add(1)
		return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody, Request: req}, nil
	}))

	for _, rawURL := range []string{
		"http://example.com",
		"https://user@example.com",
		"https://127.0.0.1",
		"https://example.com:444/path",
	} {
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, rawURL, nil)
		require.NoError(t, err)
		_, err = client.Do(req)
		require.Error(t, err)
	}
	assert.Zero(t, calls.Load())
}

func TestCandidateClientRejectsRedirects(t *testing.T) {
	var calls atomic.Int32
	client := newCandidateHTTPClientWithTransport(time.Second, 1024, roundTripFunc(func(req *http.Request) (*http.Response, error) {
		calls.Add(1)
		return &http.Response{
			StatusCode: http.StatusFound,
			Header:     http.Header{"Location": []string{"https://other.example/next"}},
			Body:       http.NoBody,
			Request:    req,
		}, nil
	}))

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://public.example/start", nil)
	require.NoError(t, err)
	resp, err := client.Do(req)
	require.ErrorIs(t, err, errCandidateRedirect)
	if resp != nil {
		resp.Body.Close()
	}
	assert.Equal(t, int32(1), calls.Load())
}

func TestCandidateBodyLimitDeclared(t *testing.T) {
	const limit = int64(16)
	body := &countingReadCloser{reader: bytes.NewReader(bytes.Repeat([]byte("x"), int(limit+1)))}
	client := newCandidateHTTPClientWithTransport(time.Second, limit, roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode:    http.StatusOK,
			ContentLength: limit + 1,
			Body:          body,
			Request:       req,
		}, nil
	}))

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://public.example/data", nil)
	require.NoError(t, err)
	_, err = client.Do(req)
	var limitErr *HTTPBodyTooLargeError
	require.ErrorAs(t, err, &limitErr)
	assert.Zero(t, body.read)
	assert.True(t, body.closed)
}

func TestCandidateBodyLimitUnknownReadsOnlyNPlusOne(t *testing.T) {
	const limit = int64(16)
	body := &countingReadCloser{reader: bytes.NewReader(bytes.Repeat([]byte("x"), int(limit+100)))}
	client := newCandidateHTTPClientWithTransport(time.Second, limit, roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode:    http.StatusOK,
			ContentLength: -1,
			Body:          body,
			Request:       req,
		}, nil
	}))

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://public.example/data", nil)
	require.NoError(t, err)
	resp, err := client.Do(req)
	require.NoError(t, err)
	data, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	var limitErr *HTTPBodyTooLargeError
	require.ErrorAs(t, err, &limitErr)
	assert.Len(t, data, int(limit))
	assert.Equal(t, limit+1, body.read)
}

func TestIdentityHTTPBodyLimits(t *testing.T) {
	did := syntax.DID("did:plc:wqgdnqlv2mwiio6pfchwtrff")
	for _, contentLength := range []int64{33, -1} {
		t.Run(strings.ReplaceAll(httpContentLengthName(contentLength), " ", "_"), func(t *testing.T) {
			body := &countingReadCloser{reader: bytes.NewReader(bytes.Repeat([]byte("x"), 64))}
			client := newCandidateHTTPClientWithTransport(time.Second, 32, roundTripFunc(func(req *http.Request) (*http.Response, error) {
				assert.Equal(t, "/"+did.String(), req.URL.Path)
				return &http.Response{StatusCode: http.StatusOK, ContentLength: contentLength, Body: body, Request: req}, nil
			}))
			directory := identity.BaseDirectory{PLCURL: "https://plc.example", HTTPClient: *client}
			_, err := directory.ResolveDID(t.Context(), did)
			var limitErr *HTTPBodyTooLargeError
			require.ErrorAs(t, err, &limitErr)
			assert.LessOrEqual(t, body.read, int64(33))
		})
	}
}

func httpContentLengthName(contentLength int64) string {
	if contentLength < 0 {
		return "chunked or unknown"
	}
	return "declared"
}
