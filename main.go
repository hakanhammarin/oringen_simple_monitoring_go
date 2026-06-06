package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/net/icmp"
	"golang.org/x/net/ipv4"
)

const (
	defaultConfigFile  = "cis.json"
	defaultResultDir   = "result"
	defaultResultFile  = "result.json"
	defaultLogDir      = "monitor-logs"
	defaultWebRoot     = "wwwroot"
	defaultHTML        = "monitoring.html"
	defaultHTTPTimeout = 2 * time.Second
	defaultPingTimeout = 600 * time.Millisecond
	defaultInterval    = 10 * time.Second
	downThreshold      = 4
	logRetentionDays   = 30
)

type MonitorEntry struct {
	Index    int    `json:"index"`
	Type     string `json:"type"`
	IP       string `json:"ip"`
	Name     string `json:"name"`
	Status   string `json:"status"`
	Time     string `json:"time"`
	RTT      string `json:"rtt"`
	Count    int    `json:"count"`
	Downtime string `json:"downtime"`
}

type rawEntry struct {
	Index    int             `json:"index"`
	Type     string          `json:"type"`
	IP       string          `json:"ip"`
	Name     string          `json:"name"`
	Status   json.RawMessage `json:"status"`
	Time     json.RawMessage `json:"time"`
	RTT      json.RawMessage `json:"rtt"`
	Count    int             `json:"count"`
	Downtime json.RawMessage `json:"downtime"`
}

func (e *MonitorEntry) UnmarshalJSON(data []byte) error {
	var raw rawEntry
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	e.Index = raw.Index
	e.Type = raw.Type
	e.IP = raw.IP
	e.Name = raw.Name
	e.Count = raw.Count
	e.Status = rawString(raw.Status)
	e.Time = rawString(raw.Time)
	e.RTT = rawString(raw.RTT)
	e.Downtime = rawString(raw.Downtime)
	return nil
}

func rawString(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s
	}
	var n json.Number
	if err := json.Unmarshal(raw, &n); err == nil {
		return n.String()
	}
	return strings.Trim(string(raw), `"`)
}

// icmpPinger holds a single shared ICMP socket. A background goroutine reads
// all incoming replies and routes each one to the waiting ping call by matching
// the ICMP echo ID and sequence number, so concurrent pings work correctly
// without racing over multiple open sockets.
//
// It tries a privileged raw socket first (ip4:icmp), then falls back to the
// unprivileged datagram socket (udp4) available on macOS without root.
type icmpPinger struct {
	conn       *icmp.PacketConn
	mu         sync.Mutex
	waiting    map[uint16]*pendingPing
	nextSeq    uint32 // accessed with sync/atomic
	id         uint16
	privileged bool // true → net.IPAddr dst, false → net.UDPAddr dst
}

type pendingPing struct {
	ch   chan time.Duration // receives RTT on reply
	sent time.Time
}

func newICMPPinger() (*icmpPinger, error) {
	conn, err := icmp.ListenPacket("ip4:icmp", "0.0.0.0")
	privileged := true
	if err != nil {
		conn, err = icmp.ListenPacket("udp4", "0.0.0.0")
		privileged = false
	}
	if err != nil {
		return nil, err
	}
	p := &icmpPinger{
		conn:       conn,
		waiting:    make(map[uint16]*pendingPing),
		id:         uint16(os.Getpid() & 0xffff),
		privileged: privileged,
	}
	go p.readLoop()
	return p, nil
}

func (p *icmpPinger) readLoop() {
	buf := make([]byte, 1500)
	for {
		n, _, err := p.conn.ReadFrom(buf)
		if err != nil {
			return
		}
		msg, err := icmp.ParseMessage(ipv4.ICMPTypeEchoReply.Protocol(), buf[:n])
		if err != nil || msg.Type != ipv4.ICMPTypeEchoReply {
			continue
		}
		echo, ok := msg.Body.(*icmp.Echo)
		if !ok || echo.ID != int(p.id) {
			continue
		}
		seq := uint16(echo.Seq)
		p.mu.Lock()
		if pending, found := p.waiting[seq]; found {
			pending.ch <- time.Since(pending.sent)
			delete(p.waiting, seq)
		}
		p.mu.Unlock()
	}
}

func (p *icmpPinger) ping(address string, timeout time.Duration) (string, string) {
	ipAddr, err := net.ResolveIPAddr("ip4", address)
	if err != nil {
		return "TimedOut", ""
	}

	seq := uint16(atomic.AddUint32(&p.nextSeq, 1))
	msg := icmp.Message{
		Type: ipv4.ICMPTypeEcho,
		Code: 0,
		Body: &icmp.Echo{
			ID:   int(p.id),
			Seq:  int(seq),
			Data: []byte("HELLO"),
		},
	}
	encoded, err := msg.Marshal(nil)
	if err != nil {
		return "TimedOut", ""
	}

	ch := make(chan time.Duration, 1)
	p.mu.Lock()
	p.waiting[seq] = &pendingPing{ch: ch, sent: time.Now()}
	p.mu.Unlock()

	var dst net.Addr
	if p.privileged {
		dst = &net.IPAddr{IP: ipAddr.IP}
	} else {
		dst = &net.UDPAddr{IP: ipAddr.IP}
	}
	if _, err := p.conn.WriteTo(encoded, dst); err != nil {
		p.mu.Lock()
		delete(p.waiting, seq)
		p.mu.Unlock()
		return "TimedOut", ""
	}

	select {
	case rtt := <-ch:
		return "Success", fmt.Sprintf("%d ms", int(rtt.Milliseconds()))
	case <-time.After(timeout):
		p.mu.Lock()
		delete(p.waiting, seq)
		p.mu.Unlock()
		return "TimedOut", ""
	}
}

func main() {
	baseDir, err := executableDir()
	if err != nil {
		fmt.Fprintf(os.Stderr, "unable to determine executable directory: %v\n", err)
		os.Exit(1)
	}

	configPath := filepath.Join(baseDir, defaultConfigFile)
	resultPath := filepath.Join(baseDir, defaultResultDir, defaultResultFile)
	htmlPath := filepath.Join(baseDir, defaultWebRoot, defaultHTML)
	logDir := filepath.Join(baseDir, defaultLogDir)

	entries, err := loadEntries(configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to load config: %v\n", err)
		os.Exit(1)
	}

	if err := os.MkdirAll(filepath.Dir(resultPath), 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "failed to create result directory: %v\n", err)
		os.Exit(1)
	}
	if err := os.MkdirAll(logDir, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "failed to create log directory: %v\n", err)
		os.Exit(1)
	}

	pinger, err := newICMPPinger()
	if err != nil {
		fmt.Fprintf(os.Stderr, "ICMP pinger unavailable, ICMP checks will be skipped: %v\n", err)
	}

	manager := &monitorManager{
		entries:    entries,
		resultPath: resultPath,
		htmlPath:   htmlPath,
		logDir:     logDir,
		pinger:     pinger,
	}

	if err := manager.persist(); err != nil {
		fmt.Fprintf(os.Stderr, "failed to write initial result file: %v\n", err)
		os.Exit(1)
	}

	go manager.startLoop()

	fmt.Println("Monitoring server starting on http://localhost:8080/")
	http.HandleFunc("/", manager.handleRoot)
	http.HandleFunc("/result/", manager.handleResult)
	if err := http.ListenAndServe(":8080", nil); err != nil {
		fmt.Fprintf(os.Stderr, "server error: %v\n", err)
		os.Exit(1)
	}
}

type monitorManager struct {
	mu         sync.RWMutex
	entries    []*MonitorEntry
	resultPath string
	htmlPath   string
	logDir     string
	pinger     *icmpPinger
}

func (m *monitorManager) startLoop() {
	ticker := time.NewTicker(defaultInterval)
	defer ticker.Stop()

	for {
		m.checkAll()
		if err := m.persist(); err != nil {
			fmt.Fprintf(os.Stderr, "failed to persist result: %v\n", err)
		}
		rotateOldLogs(m.logDir, logRetentionDays)
		<-ticker.C
	}
}

func (m *monitorManager) checkAll() {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now().Format("2006-01-02 15:04:05")
	logFile := filepath.Join(m.logDir, time.Now().Format("2006-01-02")+"-monitoring-log.csv")
	logWriter, err := os.OpenFile(logFile, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		fmt.Fprintf(os.Stderr, "unable to open log file: %v\n", err)
	}
	defer func() {
		if logWriter != nil {
			_ = logWriter.Close()
		}
	}()

	var csvWriter *csv.Writer
	if logWriter != nil {
		csvWriter = csv.NewWriter(logWriter)
		if fileEmpty(logWriter) {
			_ = csvWriter.Write([]string{"time", "name", "ip", "type", "status", "rtt", "count", "downtime"})
		}
	}

	var wg sync.WaitGroup
	for _, entry := range m.entries {
		wg.Add(1)
		go func(e *MonitorEntry) {
			defer wg.Done()
			m.checkEntry(e)
			e.Time = now
		}(entry)
	}
	wg.Wait()

	// Write log entries sequentially after all checks complete (csv.Writer is not goroutine-safe).
	if csvWriter != nil {
		for _, e := range m.entries {
			if shouldLog(e) {
				_ = csvWriter.Write([]string{e.Time, e.Name, e.IP, e.Type, e.Status, e.RTT, strconv.Itoa(e.Count), e.Downtime})
			}
		}
		csvWriter.Flush()
	}

	sortEntries(m.entries)
}

func (m *monitorManager) checkEntry(e *MonitorEntry) {
	switch strings.ToUpper(e.Type) {
	case "ICMP":
		m.checkICMP(e)
	case "OLA":
		m.checkOLA(e)
	case "FUEL":
		m.checkFuel(e)
	default:
		e.Status = "0"
		e.RTT = ""
	}
}

func (m *monitorManager) checkICMP(e *MonitorEntry) {
	if m.pinger == nil {
		e.Status = "0"
		e.RTT = ""
		return
	}
	status, rtt := m.pinger.ping(e.IP, defaultPingTimeout)
	if status == "Success" {
		e.Status = "10"
		e.Count = 0
		e.RTT = rtt
		return
	}

	e.RTT = ""
	e.Status = "20"
	if e.Count+1 >= downThreshold {
		e.Status = "40"
		if e.Count < downThreshold {
			e.Downtime = time.Now().Format("2006-01-02 15:04:05")
		}
	}
	e.Count++
}

func (m *monitorManager) checkOLA(e *MonitorEntry) {
	client := http.Client{Timeout: defaultHTTPTimeout}
	resp, err := client.Get(e.IP)
	if err != nil {
		e.onHTTPFailure()
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		e.Status = "10"
		e.Count = 0
		e.RTT = fmt.Sprintf("%d", resp.StatusCode)
		return
	}
	e.onHTTPFailure()
}

func (m *monitorManager) checkFuel(e *MonitorEntry) {
	request, err := http.NewRequest(http.MethodGet, e.IP, nil)
	if err != nil {
		e.onHTTPFailure()
		return
	}
	if user, pass, ok := fuelCredentials(); ok {
		request.SetBasicAuth(user, pass)
	}
	client := http.Client{Timeout: defaultHTTPTimeout}
	resp, err := client.Do(request)
	if err != nil {
		e.onHTTPFailure()
		return
	}
	defer resp.Body.Close()

	var payload struct {
		Fuel string `json:"fuel"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&payload)
	f, err := strconv.Atoi(payload.Fuel)
	if err != nil || payload.Fuel == "" {
		e.onHTTPFailure()
		return
	}

	e.RTT = fmt.Sprintf("%s%%", payload.Fuel)
	switch {
	case f >= 50:
		e.Status = "10"
		e.Count = 0
	case f <= 10:
		if e.Count+1 >= downThreshold && e.Count < downThreshold {
			e.Downtime = time.Now().Format("2006-01-02 15:04:05")
		}
		e.Status = "40"
		e.Count++
	default:
		e.Status = "20"
		e.Count = 0
	}
}

func (e *MonitorEntry) onHTTPFailure() {
	if e.Count+1 >= downThreshold {
		if e.Count < downThreshold {
			e.Downtime = time.Now().Format("2006-01-02 15:04:05")
		}
		e.Status = "40"
	} else {
		e.Status = "20"
	}
	e.Count++
	e.RTT = ""
}

func fuelCredentials() (string, string, bool) {
	user := os.Getenv("FUEL_API_USER")
	pass := os.Getenv("FUEL_API_PASSWORD")
	if user == "" || pass == "" {
		return "", "", false
	}
	return user, pass, true
}

func shouldLog(e *MonitorEntry) bool {
	if numericStatus(e.Status) >= 30 {
		return true
	}
	if strings.HasSuffix(e.RTT, "ms") {
		value := strings.TrimSuffix(e.RTT, "ms")
		if n, err := strconv.Atoi(strings.TrimSpace(value)); err == nil && n > 500 {
			return true
		}
	}
	return false
}

func numericStatus(status string) int {
	n, _ := strconv.Atoi(status)
	return n
}

func sortEntries(entries []*MonitorEntry) {
	sort.Slice(entries, func(i, j int) bool {
		si := numericStatus(entries[i].Status)
		sj := numericStatus(entries[j].Status)
		if si != sj {
			return si > sj
		}
		return strings.ToLower(entries[i].Name) < strings.ToLower(entries[j].Name)
	})
}

func (m *monitorManager) persist() error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	data, err := json.MarshalIndent(m.entries, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(m.resultPath, data, 0o644)
}

func (m *monitorManager) handleRoot(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	html, err := os.ReadFile(m.htmlPath)
	if err != nil {
		http.Error(w, "monitoring page not found", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(html)
}

func (m *monitorManager) handleResult(w http.ResponseWriter, r *http.Request) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	data, err := json.Marshal(m.entries)
	if err != nil {
		http.Error(w, "unable to encode result", http.StatusInternalServerError)
		return
	}
	_, _ = w.Write(data)
}

func fileEmpty(f *os.File) bool {
	if f == nil {
		return true
	}
	info, err := f.Stat()
	if err != nil {
		return true
	}
	return info.Size() == 0
}

func loadEntries(path string) ([]*MonitorEntry, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	var entries []*MonitorEntry
	if err := json.NewDecoder(file).Decode(&entries); err != nil {
		return nil, err
	}
	return entries, nil
}

func executableDir() (string, error) {
	exePath, err := os.Executable()
	if err != nil {
		return "", err
	}
	return filepath.Dir(exePath), nil
}

func rotateOldLogs(logDir string, days int) {
	cutoff := time.Now().AddDate(0, 0, -days)
	entries, err := os.ReadDir(logDir)
	if err != nil {
		return
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}
		if info.ModTime().Before(cutoff) {
			_ = os.Remove(filepath.Join(logDir, entry.Name()))
		}
	}
}
