package gae

import (
	"bufio"
	"bytes"
	"compress/flate"
	"encoding/binary"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"appengine"
	"appengine/urlfetch"
)

const (
	Version  = "1.0"
	Password = ""

	DefaultFetchMaxSize   = 1024 * 1024 * 4
	DefaultDeadline       = 20 * time.Second
	DefaultOverquotaDelay = 4 * time.Second
)

func ReadRequest(r io.Reader) (req *http.Request, err error) {
	req = new(http.Request)

	scanner := bufio.NewScanner(r)
	if scanner.Scan() {
		line := scanner.Text()
		parts := strings.Split(line, " ")
		if len(parts) != 3 {
			err = fmt.Errorf("Invaild Request Line: %#v", line)
			return
		}

		req.Method = parts[0]
		req.RequestURI = parts[1]
		req.Proto = "HTTP/1.1"
		req.ProtoMajor = 1
		req.ProtoMinor = 1

		if req.URL, err = url.Parse(req.RequestURI); err != nil {
			return
		}
		req.Host = req.URL.Host

		req.Header = http.Header{}
	}

	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			continue
		}

		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])
		req.Header.Add(key, value)
	}

	if err = scanner.Err(); err != nil {
		// ignore
	}

	if cl := req.Header.Get("Content-Length"); cl != "" {
		if req.ContentLength, err = strconv.ParseInt(cl, 10, 64); err != nil {
			return
		}
	}

	req.Host = req.URL.Host
	if req.Host == "" {
		req.Host = req.Header.Get("Host")
	}

	return
}

func handlerError(rw http.ResponseWriter, html string, code int) {
	var b bytes.Buffer
	w, err := flate.NewWriter(&b, flate.BestCompression)
	if err != nil {
		http.Error(rw, err.Error(), http.StatusBadGateway)
		return
	}

	fmt.Fprintf(w, "HTTP/1.1 %d\r\n", code)
	fmt.Fprintf(w, "Content-Type: text/html; charset=utf-8\r\n")
	fmt.Fprintf(w, "Content-Length: %d\r\n", len(html))
	io.WriteString(w, "\r\n")
	io.WriteString(w, html)
	w.Close()

	b0 := []byte{0, 0}
	binary.BigEndian.PutUint16(b0, uint16(b.Len()))

	rw.Header().Set("Content-Type", "image/gif")
	rw.Header().Set("Content-Length", strconv.Itoa(len(b0)+b.Len()))
	rw.WriteHeader(http.StatusOK)
	rw.Write(b0)
	rw.Write(b.Bytes())
}

func handler(rw http.ResponseWriter, r *http.Request) {
	var err error
	context := appengine.NewContext(r)
	context.Infof("Hanlde Request=%#v\n", r)

	var hdrLen uint16
	if err := binary.Read(r.Body, binary.BigEndian, &hdrLen); err != nil {
		context.Criticalf("binary.Read(&hdrLen) return %v", err)
		handlerError(rw, err.Error(), http.StatusBadRequest)
		return
	}

	req, err := ReadRequest(bufio.NewReader(flate.NewReader(&io.LimitedReader{R: r.Body, N: int64(hdrLen)})))
	if err != nil {
		context.Criticalf("http.ReadRequest(%#v) return %#v", r.Body, err)
		handlerError(rw, err.Error(), http.StatusBadRequest)
		return
	}

	req.Body = r.Body
	defer req.Body.Close()

	params := http.Header{}
	var paramPrefix string = http.CanonicalHeaderKey("X-UrlFetch-")
	for key, values := range req.Header {
		if strings.HasPrefix(key, paramPrefix) {
			params.Set(key, values[0])
		}
	}

	for key, _ := range params {
		req.Header.Del(key)
	}
	// req.Header.Del("X-Cloud-Trace-Context")

	context.Infof("Parsed Request=%#v\n", req)

	if Password != "" {
		password := params.Get("X-UrlFetch-Password")
		switch {
		case password == "":
			handlerError(rw, "No Password.", http.StatusForbidden)
			return
		case password != Password:
			handlerError(rw, "Wrong Password.", http.StatusForbidden)
			return
		}
	}

	deadline := DefaultDeadline
	if s := params.Get("X-UrlFetch-Deadline"); s != "" {
		if n, err := strconv.Atoi(s); err == nil {
			deadline = time.Duration(n) * time.Second
		}
	}

	overquotaDelay := DefaultOverquotaDelay
	if s := params.Get("X-UrlFetch-OverquotaDelay"); s != "" {
		if n, err := strconv.Atoi(s); err == nil {
			deadline = time.Duration(n) * time.Second
		}
	}

	fetchMaxSize := DefaultFetchMaxSize
	if s := params.Get("X-UrlFetch-MaxSize"); s != "" {
		if n, err := strconv.Atoi(s); err == nil {
			fetchMaxSize = n
		}
	}

	var resp *http.Response
	for i := 0; i < 2; i++ {
		t := &urlfetch.Transport{
			Context:                       context,
			Deadline:                      deadline,
			AllowInvalidServerCertificate: true,
		}

		resp, err = t.RoundTrip(req)
		if err == nil {
			defer resp.Body.Close()
			break
		}

		message := err.Error()
		if strings.Contains(message, "RESPONSE_TOO_LARGE") {
			context.Warningf("URLFetchServiceError %T(%v) deadline=%v, url=%v", err, err, deadline, req.URL.String())
			if s := req.Header.Get("Range"); s != "" {
				if parts1 := strings.Split(s, "="); len(parts1) == 2 {
					if parts2 := strings.Split(parts1[1], "-"); len(parts2) == 2 {
						if start, err1 := strconv.Atoi(parts2[0]); err1 == nil {
							end, err1 := strconv.Atoi(parts2[1])
							if err1 != nil {
								req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, start+fetchMaxSize))
							} else {
								if end-start > fetchMaxSize {
									req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, start+fetchMaxSize))
								} else {
									req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))
								}
							}
						}
					}
				}
			} else {
				req.Header.Set("Range", fmt.Sprintf("bytes=0-%d", fetchMaxSize))
			}
		} else if strings.Contains(message, "Over quota") {
			context.Warningf("URLFetchServiceError %T(%v) deadline=%v, url=%v", err, err, deadline, req.URL.String())
			time.Sleep(overquotaDelay)
		} else {
			context.Errorf("URLFetchServiceError %T(%v) deadline=%v, url=%v", err, err, deadline, req.URL.String())
			break
		}
	}

	if err != nil {
		handlerError(rw, fmt.Sprintf("Go Server Fetch Failed: %v", err), http.StatusBadGateway)
		return
	}

	// rewise resp.Header
	resp.Header.Del("Transfer-Encoding")
	if strings.ToLower(resp.Header.Get("Vary")) == "accept-encoding" {
		resp.Header.Del("Vary")
	}
	if resp.ContentLength > 0 {
		resp.Header.Set("Content-Length", strconv.FormatInt(resp.ContentLength, 10))
	}

	// Fix Set-Cookie header for python client
	if r.Header.Get("User-Agent") == "" {
		const cookieHeader string = "Set-Cookie"
		if resp.Header.Get(cookieHeader) != "" {
			resp.Header[cookieHeader] = []string{strings.Join(resp.Header[cookieHeader], ", ")}
		}
	}

	context.Infof("Write Response=%#v\n", resp)

	var b bytes.Buffer
	w, err := flate.NewWriter(&b, flate.BestCompression)
	if err != nil {
		handlerError(rw, fmt.Sprintf("Go Server Fetch Failed: %v", w), http.StatusBadGateway)
		return
	}

	fmt.Fprintf(w, "HTTP/1.1 %d %s\r\n", resp.StatusCode, resp.Status)
	resp.Header.Write(w)
	io.WriteString(w, "\r\n")
	w.Close()

	b0 := []byte{0, 0}
	binary.BigEndian.PutUint16(b0, uint16(b.Len()))

	rw.Header().Set("Content-Type", "image/gif")
	rw.Header().Set("Content-Length", strconv.FormatInt(int64(len(b0)+b.Len())+resp.ContentLength, 10))
	rw.WriteHeader(http.StatusOK)
	rw.Write(b0)
	io.Copy(rw, io.MultiReader(&b, resp.Body))
}

func favicon(rw http.ResponseWriter, r *http.Request) {
	rw.WriteHeader(http.StatusOK)
}

func robots(rw http.ResponseWriter, r *http.Request) {
	rw.Header().Set("Content-Type", "text/plain; charset=utf-8")
	io.WriteString(rw, "User-agent: *\nDisallow: /\n")
}

func root(rw http.ResponseWriter, r *http.Request) {
	context := appengine.NewContext(r)
	version, _ := strconv.ParseInt(strings.Split(appengine.VersionID(context), ".")[1], 10, 64)
	ctime := time.Unix(version/(1<<28), 0).Format(time.RFC3339)
	rw.Header().Set("Content-Type", "text/html; charset=utf-8")
	fmt.Fprintf(rw, `
<html>
<head>
	<title>GoProxy</title>
</head>
<body>
<div>GoProxy server %s works, deployed at %s, latest version is <span id="github"></span></div>
</body>
<script type="text/javascript">
	function callback(commits) {
		date = commits['data'][0]['commit']['committer']['date']
		document.getElementById('github').innerHTML = date
	}
</script>
<script src="https://api.github.com/repos/phuslu/goproxy/commits?sha=server.gae&path=gae/gae.go&callback=callback"></script>
</html>
`, Version, ctime)
}

func init() {
	http.HandleFunc("/_gh/", handler)
	http.HandleFunc("/favicon.ico", favicon)
	http.HandleFunc("/robots.txt", robots)
	http.HandleFunc("/", root)
}
