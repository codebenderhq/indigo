package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
	"syscall"
	"time"

	"github.com/bluesky-social/indigo/util/ssrf"
)

const (
	defaultIdentityMaxBytes int64 = 1 << 20
	defaultRepoMaxBytes     int64 = 64 << 20
	defaultRepoMaxBlocks    int64 = 100_000

	maxIdentityMaxBytes int64 = 16 << 20
	maxRepoMaxBytes     int64 = 512 << 20
	maxRepoMaxBlocks    int64 = 500_000

	identityHTTPTimeout = 10 * time.Second
)

var errCandidateRedirect = errors.New("tap candidate HTTP redirects are disabled")

var deniedCandidateIPv6Prefixes = []netip.Prefix{
	netip.MustParsePrefix("::/96"),          // IPv4-compatible, unspecified, and loopback space
	netip.MustParsePrefix("64:ff9b::/96"),   // IPv4/IPv6 translation
	netip.MustParsePrefix("64:ff9b:1::/48"), // local-use IPv4/IPv6 translation
	netip.MustParsePrefix("100::/64"),       // discard-only
	netip.MustParsePrefix("2001::/23"),      // IETF special-purpose, including benchmarking and ORCHID
	netip.MustParsePrefix("2001:db8::/32"),  // documentation
	netip.MustParsePrefix("2002::/16"),      // 6to4
	netip.MustParsePrefix("3fff::/20"),      // documentation
	netip.MustParsePrefix("5f00::/16"),      // segment-routing special-purpose
	netip.MustParsePrefix("fc00::/7"),       // unique-local
	netip.MustParsePrefix("fe80::/10"),      // link-local
	netip.MustParsePrefix("fec0::/10"),      // deprecated site-local
	netip.MustParsePrefix("ff00::/8"),       // multicast
}

// HTTPBodyTooLargeError is returned before an HTTP response body can grow past
// the configured candidate-fetch limit.
type HTTPBodyTooLargeError struct {
	Limit    int64
	Declared int64
}

func (e *HTTPBodyTooLargeError) Error() string {
	if e.Declared >= 0 {
		return fmt.Sprintf("HTTP response body is too large: declared %d bytes, limit %d bytes", e.Declared, e.Limit)
	}
	return fmt.Sprintf("HTTP response body is too large: limit %d bytes", e.Limit)
}

func isHTTPBodyTooLarge(err error) bool {
	var limitErr *HTTPBodyTooLargeError
	return errors.As(err, &limitErr)
}

type candidateRoundTripper struct {
	base     http.RoundTripper
	maxBytes int64
}

func newCandidateHTTPClient(timeout time.Duration, maxBytes int64) *http.Client {
	transport := ssrf.PublicOnlyTransport()
	transport.Proxy = nil
	transport.DisableCompression = true

	publicOnlyDialer := newCandidatePublicOnlyDialer(nil)
	publicOnlyDial := publicOnlyDialer.DialContext
	transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		if network != "tcp" && network != "tcp4" && network != "tcp6" {
			return nil, fmt.Errorf("%s is not a safe network type for Tap candidate fetches", network)
		}
		_, port, err := net.SplitHostPort(address)
		if err != nil {
			return nil, fmt.Errorf("invalid Tap candidate dial address %q: %w", address, err)
		}
		if port != "443" {
			return nil, fmt.Errorf("%s is not a safe port for Tap candidate fetches", port)
		}
		return publicOnlyDial(ctx, network, address)
	}

	return newCandidateHTTPClientWithTransport(timeout, maxBytes, transport)
}

func newCandidatePublicOnlyDialer(resolver *net.Resolver) *net.Dialer {
	dialer := ssrf.PublicOnlyDialer()
	dialer.Resolver = resolver
	dialer.Control = candidatePublicOnlyControl
	return dialer
}

func newCandidateHTTPClientWithTransport(timeout time.Duration, maxBytes int64, transport http.RoundTripper) *http.Client {
	return &http.Client{
		Transport: &candidateRoundTripper{
			base:     transport,
			maxBytes: maxBytes,
		},
		Timeout: timeout,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return errCandidateRedirect
		},
	}
}

func (t *candidateRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if req == nil || req.URL == nil {
		return nil, fmt.Errorf("Tap candidate request URL is required")
	}
	if t.maxBytes <= 0 {
		return nil, fmt.Errorf("Tap candidate response limit must be positive")
	}
	if err := validateCandidateURL(req.URL); err != nil {
		return nil, err
	}

	resp, err := t.base.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	if resp.ContentLength > t.maxBytes {
		if resp.Body != nil {
			resp.Body.Close()
		}
		return nil, &HTTPBodyTooLargeError{Limit: t.maxBytes, Declared: resp.ContentLength}
	}
	if resp.Body != nil {
		resp.Body = &limitedResponseBody{
			ReadCloser: resp.Body,
			limit:      t.maxBytes,
			declared:   resp.ContentLength,
		}
	}
	return resp, nil
}

type limitedResponseBody struct {
	io.ReadCloser
	limit    int64
	read     int64
	declared int64
}

func (b *limitedResponseBody) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}

	if b.read < b.limit {
		remaining := b.limit - b.read
		if int64(len(p)) > remaining {
			p = p[:remaining]
		}
		n, err := b.ReadCloser.Read(p)
		b.read += int64(n)
		return n, err
	}

	var probe [1]byte
	n, err := b.ReadCloser.Read(probe[:])
	if n > 0 {
		return 0, &HTTPBodyTooLargeError{Limit: b.limit, Declared: b.declared}
	}
	return 0, err
}

func validateCandidateOrigin(rawURL string) error {
	if strings.Contains(rawURL, "#") {
		return fmt.Errorf("URL fragments are not allowed")
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("invalid URL: %w", err)
	}
	if err := validateCandidateURL(u); err != nil {
		return err
	}
	if escapedPath := u.EscapedPath(); escapedPath != "" && escapedPath != "/" {
		return fmt.Errorf("URL must be a pathless HTTPS origin")
	}
	if u.RawQuery != "" || u.ForceQuery {
		return fmt.Errorf("URL queries are not allowed")
	}
	return nil
}

func canonicalCandidateOrigin(rawURL string) (string, error) {
	if err := validateCandidateOrigin(rawURL); err != nil {
		return "", err
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", err
	}
	if u.Path == "/" {
		u.Path = ""
		u.RawPath = ""
	}
	return u.String(), nil
}

func validateCandidateURL(u *url.URL) error {
	if !u.IsAbs() || u.Scheme != "https" || u.Opaque != "" {
		return fmt.Errorf("URL must be absolute HTTPS")
	}
	if u.User != nil {
		return fmt.Errorf("URL credentials are not allowed")
	}
	if u.Fragment != "" || u.RawFragment != "" {
		return fmt.Errorf("URL fragments are not allowed")
	}

	hostname := u.Hostname()
	if hostname == "" {
		return fmt.Errorf("URL hostname is required")
	}
	if strings.Contains(hostname, "%") {
		return fmt.Errorf("IPv6 zones are not allowed")
	}

	expectedHost := hostname
	if strings.Contains(hostname, ":") {
		expectedHost = "[" + hostname + "]"
	}
	switch u.Host {
	case expectedHost:
	case expectedHost + ":443":
	default:
		return fmt.Errorf("URL port must be absent or 443")
	}

	normalizedHost := strings.TrimSuffix(strings.ToLower(hostname), ".")
	if normalizedHost == "localhost" || strings.HasSuffix(normalizedHost, ".localhost") {
		return fmt.Errorf("localhost is not allowed")
	}
	if ip := net.ParseIP(normalizedHost); ip != nil && !isCandidatePublicIP(ip) {
		return fmt.Errorf("%s is not a public IP address", ip)
	}
	return nil
}

func candidatePublicOnlyControl(network, address string, conn syscall.RawConn) error {
	if err := ssrf.PublicOnlyControl(network, address, conn); err != nil {
		return err
	}
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return err
	}
	ip := net.ParseIP(host)
	if ip == nil || !isCandidatePublicIP(ip) {
		return fmt.Errorf("%s is not a public IP address", host)
	}
	return nil
}

func isCandidatePublicIP(ip net.IP) bool {
	if ip.To4() != nil {
		return ssrf.IsPublicIPAddress(ip)
	}
	addr, ok := netip.AddrFromSlice(ip)
	if !ok || !addr.Is6() || !addr.IsGlobalUnicast() {
		return false
	}
	for _, prefix := range deniedCandidateIPv6Prefixes {
		if prefix.Contains(addr) {
			return false
		}
	}
	return true
}

func validateByteLimit(name string, value, ceiling int64) error {
	if value <= 0 {
		return fmt.Errorf("%s must be a positive number of bytes", name)
	}
	if value > ceiling {
		return fmt.Errorf("%s must not exceed %d bytes", name, ceiling)
	}
	return nil
}

func validateCountLimit(name string, value, ceiling int64) error {
	if value <= 0 {
		return fmt.Errorf("%s must be a positive integer", name)
	}
	if value > ceiling {
		return fmt.Errorf("%s must not exceed %d", name, ceiling)
	}
	return nil
}
