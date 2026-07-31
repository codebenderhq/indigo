package main

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"runtime"

	"github.com/bluesky-social/indigo/atproto/atclient"
	"github.com/bluesky-social/indigo/atproto/identity"
)

const (
	atprotoPDSServiceType = "AtprotoPersonalDataServer"
	maxCARHeaderBytes     = 1 << 20
)

type CARBlockLimitError struct {
	Limit int64
}

func (e *CARBlockLimitError) Error() string {
	return fmt.Sprintf("CAR contains too many blocks: limit %d", e.Limit)
}

func isTerminalResyncError(err error) bool {
	var blockLimitErr *CARBlockLimitError
	return isHTTPBodyTooLarge(err) || errors.As(err, &blockLimitErr)
}

func candidatePDSEndpoint(ident *identity.Identity) (string, error) {
	service, ok := ident.Services["atproto_pds"]
	if !ok {
		return "", fmt.Errorf("no PDS endpoint for DID: %s", ident.DID)
	}
	if service.Type != atprotoPDSServiceType {
		return "", fmt.Errorf("PDS service for DID %s has invalid type %q", ident.DID, service.Type)
	}
	endpoint, err := canonicalCandidateOrigin(service.URL)
	if err != nil {
		return "", fmt.Errorf("invalid PDS endpoint for DID %s: %w", ident.DID, err)
	}
	return endpoint, nil
}

func (r *Resyncer) fetchRepoCAR(ctx context.Context, pdsURL, did string) (*os.File, int64, error) {
	u, err := url.Parse(pdsURL)
	if err != nil {
		return nil, 0, err
	}
	u.Path = "/xrpc/com.atproto.sync.getRepo"
	query := u.Query()
	query.Set("did", did)
	u.RawQuery = query.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, 0, fmt.Errorf("creating getRepo request: %w", err)
	}
	req.Header.Set("Accept", "application/vnd.ipld.car")
	req.Header.Set("User-Agent", userAgent())

	resp, err := r.repoHTTPClient.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return nil, 0, &atclient.APIError{StatusCode: resp.StatusCode}
	}
	if resp.ContentLength > r.repoMaxBytes {
		return nil, 0, &HTTPBodyTooLargeError{Limit: r.repoMaxBytes, Declared: resp.ContentLength}
	}

	file, err := createUnlinkedRepoTemp(r.repoTempDir)
	if err != nil {
		return nil, 0, fmt.Errorf("creating repo temp file: %w", err)
	}
	remove := true
	defer func() {
		if remove {
			if closeErr := file.Close(); closeErr != nil && r.logger != nil {
				r.logger.Error("failed to close repo temp file", "error", closeErr)
			}
		}
	}()

	body := &limitedResponseBody{
		ReadCloser: resp.Body,
		limit:      r.repoMaxBytes,
		declared:   resp.ContentLength,
	}
	written, err := io.Copy(file, body)
	if err != nil {
		return nil, 0, fmt.Errorf("streaming repo CAR: %w", err)
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return nil, 0, fmt.Errorf("rewinding repo CAR: %w", err)
	}
	remove = false
	return file, written, nil
}

func createUnlinkedRepoTemp(dir string) (*os.File, error) {
	switch runtime.GOOS {
	case "aix", "darwin", "dragonfly", "freebsd", "linux", "netbsd", "openbsd", "solaris":
	default:
		return nil, fmt.Errorf("unlinked temporary files are unsupported on %s", runtime.GOOS)
	}
	file, err := os.CreateTemp(dir, "tap-repo-*.car")
	if err != nil {
		return nil, err
	}
	if err := os.Remove(file.Name()); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("unlinking repo temp file: %w", err)
	}
	return file, nil
}

func cleanupRepoFile(file *os.File) error {
	return file.Close()
}

func preflightCARBlocks(ctx context.Context, file *os.File, maxBlocks int64) (int64, error) {
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}
	reader := bufio.NewReader(file)
	headerSize, err := binary.ReadUvarint(reader)
	if err != nil {
		return 0, fmt.Errorf("reading CAR header length: %w", err)
	}
	if headerSize == 0 || headerSize > maxCARHeaderBytes {
		return 0, fmt.Errorf("invalid CAR header size %d", headerSize)
	}
	if err := discardCARSection(ctx, reader, headerSize); err != nil {
		return 0, fmt.Errorf("reading CAR header: %w", err)
	}

	var blocks int64
	for {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		sectionSize, err := binary.ReadUvarint(reader)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return 0, fmt.Errorf("reading CAR block length: %w", err)
		}
		if sectionSize == 0 {
			return 0, fmt.Errorf("invalid empty CAR block section")
		}
		blocks++
		if blocks > maxBlocks {
			return 0, &CARBlockLimitError{Limit: maxBlocks}
		}
		if err := discardCARSection(ctx, reader, sectionSize); err != nil {
			return 0, fmt.Errorf("reading CAR block %d: %w", blocks, err)
		}
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}
	return blocks, nil
}

func discardCARSection(ctx context.Context, reader io.Reader, size uint64) error {
	if size > math.MaxInt64 {
		return fmt.Errorf("CAR section is too large")
	}
	written, err := io.CopyN(io.Discard, &contextReader{ctx: ctx, reader: reader}, int64(size))
	if err != nil {
		return err
	}
	if written != int64(size) {
		return io.ErrUnexpectedEOF
	}
	return nil
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r *contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(p)
}
