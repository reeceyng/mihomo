package outboundgroup

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/metacubex/mihomo/common/atomic"
	"github.com/metacubex/mihomo/common/singledo"
	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"
	"github.com/metacubex/mihomo/log"
)

// configFloat extracts a float64 from a config value that may be float64 or int.
func configFloat(v any) (float64, bool) {
	switch f := v.(type) {
	case float64:
		return f, true
	case int:
		return float64(f), true
	}
	return 0, false
}

func clampAlpha(v float64) float64 {
	if v < 0.01 {
		return 0.01
	}
	if v > 1.0 {
		return 1.0
	}
	return v
}

func clampTolerance(v float64) float64 {
	if v < 0 {
		return 0
	}
	if v > 1.0 {
		return 1.0
	}
	return v
}

func clampInt(v, lo, hi int) int {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

type Smart struct {
	*GroupBase
	selected       atomic.TypedValue[string]
	testUrl        string
	expectedStatus string
	tolerance      float64
	disableUDP     bool
	Hidden         bool
	Icon           string
	fastNode       C.Proxy
	fastNodeMu     sync.RWMutex
	fastSingle     *singledo.Single[C.Proxy]

	alphaUp   float64
	alphaDown float64

	stats   map[string]*proxyStats
	statsMu sync.RWMutex

	retryTimeout time.Duration

	retestInterval time.Duration
	retestSingle   *singledo.Single[struct{}]
	retestBackoff  time.Duration
	nextRetestAt   time.Time
	retestMu       sync.Mutex

	jitter jitterDetector
}

func (s *Smart) Now() string {
	p := s.fast(false)
	if p == nil {
		return ""
	}
	return p.Name()
}

func (s *Smart) Set(name string) error {
	for _, proxy := range s.GetProxies(false) {
		if proxy.Name() == name {
			s.ForceSet(name)
			return nil
		}
	}
	return errors.New("proxy not exist")
}

func (s *Smart) ForceSet(name string) {
	s.selected.Store(name)
	s.fastSingle.Reset()
}

// selectBest returns the single best proxy by score.
func (s *Smart) selectBest(touch bool) C.Proxy {
	proxies := s.GetProxies(touch)
	if len(proxies) == 0 {
		return nil
	}

	s.statsMu.RLock()
	scores := make(map[string]float64, len(proxies))
	bestProxy := proxies[0]
	bestScore := s.computeScore(bestProxy)
	scores[bestProxy.Name()] = bestScore
	for _, p := range proxies[1:] {
		sc := s.computeScore(p)
		scores[p.Name()] = sc
		if sc < bestScore {
			bestProxy = p
			bestScore = sc
		}
	}
	s.statsMu.RUnlock()

	s.fastNodeMu.Lock()
	currentFast := s.fastNode

	// Tolerance-based stickiness: keep current node if within tolerance of best.
	primary := bestProxy
	if currentFast != nil {
		if cs, ok := scores[currentFast.Name()]; ok && cs <= bestScore+s.tolerance {
			primary = currentFast
		}
	}
	if currentFast != nil && primary.Name() != currentFast.Name() {
		log.Debugln("[Smart] %s node switch: %s -> %s", s.Name(), currentFast.Name(), primary.Name())
	}
	s.fastNode = primary
	s.fastNodeMu.Unlock()

	return primary
}

// fast returns the best proxy, using singledo for dedup.
func (s *Smart) fast(touch bool) C.Proxy {
	elm, _, shared := s.fastSingle.Do(func() (C.Proxy, error) {
		if sel := s.selected.Load(); sel != "" {
			for _, proxy := range s.GetProxies(touch) {
				if proxy.Name() == sel && proxy.AliveForTestUrl(s.testUrl) {
					s.fastNodeMu.Lock()
					s.fastNode = proxy
					s.fastNodeMu.Unlock()
					return proxy, nil
				}
			}
		}
		return s.selectBest(touch), nil
	})
	if shared && touch {
		s.Touch()
	}
	return elm
}

// handleDialError resets state and records the failure for retest scheduling.
func (s *Smart) handleDialError(proxy C.Proxy, err error) {
	s.fastSingle.Reset()
	s.onDialFailed(proxy.Type(), err, s.triggerRetest)
}

// DialContext implements C.ProxyAdapter
func (s *Smart) DialContext(ctx context.Context, metadata *C.Metadata) (C.Conn, error) {
	proxy := s.fast(true)
	if proxy == nil {
		return nil, errors.New("smart: no available proxy")
	}
	c, err := proxy.DialContext(ctx, metadata)
	if err != nil {
		s.handleDialError(proxy, err)
		return nil, err
	}
	c.AppendToChains(s)
	return newSmartTrackedConn(c, s, proxy.Name()), nil
}

// ListenPacketContext implements C.ProxyAdapter
func (s *Smart) ListenPacketContext(ctx context.Context, metadata *C.Metadata) (C.PacketConn, error) {
	proxy := s.fast(true)
	if proxy == nil {
		return nil, errors.New("smart: no available proxy")
	}
	pc, err := proxy.ListenPacketContext(ctx, metadata)
	if err != nil {
		s.handleDialError(proxy, err)
		return nil, err
	}
	pc.AppendToChains(s)
	return newSmartTrackedPacketConn(pc, s, proxy.Name()), nil
}

// Unwrap implements C.ProxyAdapter
func (s *Smart) Unwrap(metadata *C.Metadata, touch bool) C.Proxy {
	return s.fast(touch)
}

func (s *Smart) healthCheck() {
	// Read results already stored by the provider-layer health check.
	proxies := s.GetProxies(false)
	current := make(map[string]struct{}, len(proxies))
	for _, p := range proxies {
		name := p.Name()
		if _, dup := current[name]; dup {
			log.Warnln("[Smart] %s duplicate proxy name %q — stats shared across providers, scoring may be inaccurate", s.Name(), name)
		}
		current[name] = struct{}{}

		if p.AliveForTestUrl(s.testUrl) {
			if delay := p.LastDelayForTestUrl(s.testUrl); isValidDelay(delay) {
				s.recordRTT(name, time.Duration(delay)*time.Millisecond, false)
			}
		} else {
			s.recordRTT(name, s.retryTimeout, true)
		}
	}
	s.fastSingle.Reset()

	// Clean up stats for removed proxies.
	s.statsMu.Lock()
	for name, st := range s.stats {
		if _, ok := current[name]; !ok && st.activeConns.Load() <= 0 {
			delete(s.stats, name)
		}
	}
	s.statsMu.Unlock()
}

// SupportUDP implements C.ProxyAdapter
func (s *Smart) SupportUDP() bool {
	p := s.fast(false)
	return !s.disableUDP && p != nil && p.SupportUDP()
}

// IsL3Protocol implements C.ProxyAdapter
func (s *Smart) IsL3Protocol(metadata *C.Metadata) bool {
	p := s.fast(false)
	return p != nil && p.IsL3Protocol(metadata)
}

// MarshalJSON implements C.ProxyAdapter
func (s *Smart) MarshalJSON() ([]byte, error) {
	proxies := s.GetProxies(false)
	all := make([]string, 0, len(proxies))
	for _, proxy := range proxies {
		all = append(all, proxy.Name())
	}

	return json.Marshal(map[string]any{
		"type":           s.Type().String(),
		"now":            s.Now(),
		"all":            all,
		"testUrl":        s.testUrl,
		"expectedStatus": s.expectedStatus,
		"fixed":          s.selected.Load(),
		"hidden":         s.Hidden,
		"icon":           s.Icon,
	})
}

func (s *Smart) Providers() []P.ProxyProvider {
	return s.providers
}

func (s *Smart) Proxies() []C.Proxy {
	return s.GetProxies(false)
}

func NewSmart(option *GroupCommonOption, providers []P.ProxyProvider, config map[string]any) *Smart {
	s := &Smart{
		GroupBase: NewGroupBase(GroupBaseOption{
			Name:           option.Name,
			Type:           C.Smart,
			Filter:         option.Filter,
			ExcludeFilter:  option.ExcludeFilter,
			ExcludeType:    option.ExcludeType,
			TestTimeout:    option.TestTimeout,
			MaxFailedTimes: option.MaxFailedTimes,
			Providers:      providers,
		}),
		fastSingle:     singledo.NewSingle[C.Proxy](time.Second * 3),
		disableUDP:     option.DisableUDP,
		testUrl:        option.URL,
		expectedStatus: option.ExpectedStatus,
		Hidden:         option.Hidden,
		Icon:           option.Icon,
		tolerance:      0.08,
		alphaUp:        0.3,
		alphaDown:      0.18,
		stats:          make(map[string]*proxyStats),
		retestInterval: 45 * time.Second,
		retestSingle:   singledo.NewSingle[struct{}](1 * time.Second),
		retestBackoff:  45 * time.Second,
		nextRetestAt:   time.Now(),
		retryTimeout:   3 * time.Second,
		jitter:         newJitterDetector(),
	}

	// Apply optional config overrides
	if f, ok := configFloat(config["tolerance"]); ok {
		s.tolerance = clampTolerance(f)
	}
	if f, ok := configFloat(config["alpha-up"]); ok {
		s.alphaUp = clampAlpha(f)
	}
	if f, ok := configFloat(config["alpha-down"]); ok {
		s.alphaDown = clampAlpha(f)
	}
	if f, ok := configFloat(config["retry-timeout"]); ok {
		d := time.Duration(f) * time.Millisecond
		if d < 1000*time.Millisecond {
			d = 1000 * time.Millisecond
		}
		s.retryTimeout = d
	}
	if f, ok := configFloat(config["retest-interval"]); ok {
		s.retestInterval = time.Duration(f) * time.Second
		s.retestBackoff = s.retestInterval
	}

	for _, pd := range providers {
		pd.RegisterAfterHealthCheckCallback(s.healthCheck)
	}

	return s
}
