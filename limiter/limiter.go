package limiter

import (
	"errors"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/InazumaV/V2bX/api/panel"
	"github.com/InazumaV/V2bX/common/format"
	"github.com/InazumaV/V2bX/conf"
	"github.com/juju/ratelimit"
)

var limitLock sync.RWMutex
var limiter map[string]*Limiter

func Init() {
	limiter = map[string]*Limiter{}
}

type Limiter struct {
	DomainRules   []*regexp.Regexp
	ProtocolRules []string
	SpeedLimit    int
	UserOnlineIP  *sync.Map      // Key: TagUUID, value: {Key: Ip, value: Uid}
	OldUserOnline *sync.Map      // Key: Ip, value: Uid
	UUIDtoUID     map[string]int // Key: UUID, value: Uid
	UserLimitInfo *sync.Map      // Key: TagUUID value: UserLimitInfo
	aliveMu       sync.RWMutex   // guards AliveList
	AliveList     map[int]int    // Key: Uid, value: alive_ip
	bucketMu      sync.Mutex     // serializes SpeedLimiter get-or-create/replace
	SpeedLimiter  *sync.Map      // key: TagUUID, value: *ratelimit.Bucket
}

type UserLimitInfo struct {
	UID         int
	SpeedLimit  int
	DeviceLimit int
	// mu guards the fields below: UpdateDynamicSpeedLimit (report task),
	// CheckLimit (per-connection) and the hy2 hooks run on different
	// goroutines and all touch them. UID/SpeedLimit/DeviceLimit are only
	// written before the entry becomes visible in UserLimitInfo, so they are
	// effectively immutable.
	mu                sync.RWMutex
	DynamicSpeedLimit int
	ExpireTime        int64
	OverLimit         bool
}

// SetDynamic stores a new dynamic limit and its expiry (unix seconds).
func (u *UserLimitInfo) SetDynamic(limit int, expire int64) {
	u.mu.Lock()
	u.DynamicSpeedLimit = limit
	u.ExpireTime = expire
	u.mu.Unlock()
}

// ExpireDynamic revokes the dynamic restriction and restores the base policy.
func (u *UserLimitInfo) ExpireDynamic() {
	u.mu.Lock()
	u.DynamicSpeedLimit = 0
	u.ExpireTime = 0
	u.mu.Unlock()
}

// DynamicState returns the current dynamic limit and expiry (unix seconds).
func (u *UserLimitInfo) DynamicState() (int, int64) {
	u.mu.RLock()
	defer u.mu.RUnlock()
	return u.DynamicSpeedLimit, u.ExpireTime
}

// SetOverLimit records whether the connection layer flagged the user.
func (u *UserLimitInfo) SetOverLimit(b bool) {
	u.mu.Lock()
	u.OverLimit = b
	u.mu.Unlock()
}

// TakeOverLimit reads and clears the over-limit flag (hy2 LogTraffic).
func (u *UserLimitInfo) TakeOverLimit() bool {
	u.mu.Lock()
	defer u.mu.Unlock()
	v := u.OverLimit
	u.OverLimit = false
	return v
}

func AddLimiter(tag string, l *conf.LimitConfig, users []panel.UserInfo, aliveList map[int]int) *Limiter {
	info := &Limiter{
		SpeedLimit:    l.SpeedLimit,
		UserOnlineIP:  new(sync.Map),
		UserLimitInfo: new(sync.Map),
		SpeedLimiter:  new(sync.Map),
		AliveList:     aliveList,
		OldUserOnline: new(sync.Map),
	}
	uuidmap := make(map[string]int)
	for i := range users {
		uuidmap[users[i].Uuid] = users[i].Id
		userLimit := &UserLimitInfo{}
		userLimit.UID = users[i].Id
		if users[i].SpeedLimit != 0 {
			userLimit.SpeedLimit = users[i].SpeedLimit
		}
		if users[i].DeviceLimit != 0 {
			userLimit.DeviceLimit = users[i].DeviceLimit
		}
		userLimit.OverLimit = false
		info.UserLimitInfo.Store(format.UserTag(tag, users[i].Uuid), userLimit)
	}
	info.UUIDtoUID = uuidmap
	limitLock.Lock()
	limiter[tag] = info
	limitLock.Unlock()
	return info
}

func GetLimiter(tag string) (info *Limiter, err error) {
	limitLock.RLock()
	info, ok := limiter[tag]
	limitLock.RUnlock()
	if !ok {
		return nil, errors.New("not found")
	}
	return info, nil
}

func DeleteLimiter(tag string) {
	limitLock.Lock()
	delete(limiter, tag)
	limitLock.Unlock()
}

// SetAliveList atomically replaces the alive-ip list; the node monitor swaps
// it while per-connection limit checks are reading the old one.
func (l *Limiter) SetAliveList(aliveList map[int]int) {
	l.aliveMu.Lock()
	l.AliveList = aliveList
	l.aliveMu.Unlock()
}

func (l *Limiter) UpdateUser(tag string, added []panel.UserInfo, deleted []panel.UserInfo) {
	for i := range deleted {
		l.UserLimitInfo.Delete(format.UserTag(tag, deleted[i].Uuid))
		l.UserOnlineIP.Delete(format.UserTag(tag, deleted[i].Uuid))
		l.SpeedLimiter.Delete(format.UserTag(tag, deleted[i].Uuid))
		delete(l.UUIDtoUID, deleted[i].Uuid)
		l.aliveMu.Lock()
		delete(l.AliveList, deleted[i].Id)
		l.aliveMu.Unlock()
	}
	for i := range added {
		userLimit := &UserLimitInfo{
			UID: added[i].Id,
		}
		if added[i].SpeedLimit != 0 {
			userLimit.SpeedLimit = added[i].SpeedLimit
			userLimit.ExpireTime = 0
		}
		if added[i].DeviceLimit != 0 {
			userLimit.DeviceLimit = added[i].DeviceLimit
		}
		userLimit.OverLimit = false
		l.UserLimitInfo.Store(format.UserTag(tag, added[i].Uuid), userLimit)
		l.UUIDtoUID[added[i].Uuid] = added[i].Id
	}
}

func (l *Limiter) UpdateDynamicSpeedLimit(tag, uuid string, limit int, expire time.Time) error {
	if v, ok := l.UserLimitInfo.Load(format.UserTag(tag, uuid)); ok {
		v.(*UserLimitInfo).SetDynamic(limit, expire.Unix())
	} else {
		return errors.New("not found")
	}
	return nil
}

func (l *Limiter) CheckLimit(taguuid string, ip string, isTcp bool, noSSUDP bool) (Bucket *ratelimit.Bucket, Reject bool) {
	// check if ipv4 mapped ipv6
	ip = strings.TrimPrefix(ip, "::ffff:")

	// check and gen speed limit Bucket
	nodeLimit := l.SpeedLimit
	userLimit := 0
	deviceLimit := 0
	var uid int
	if v, ok := l.UserLimitInfo.Load(taguuid); ok {
		u := v.(*UserLimitInfo)
		deviceLimit = u.DeviceLimit
		uid = u.UID
		dyn, exp := u.DynamicState()
		if exp != 0 && exp < time.Now().Unix() {
			// Expired dynamic limit: revoke only the dynamic restriction and
			// fall back to the base policy. The user entry must survive —
			// deleting it locked a still-valid user (SpeedLimit == 0) out of
			// the node on their next connection.
			u.ExpireDynamic()
			userLimit = u.SpeedLimit
		} else {
			userLimit = determineSpeedLimit(u.SpeedLimit, dyn)
		}
	} else {
		return nil, true
	}
	if noSSUDP {
		// Store online user for device limit
		newipMap := new(sync.Map)
		newipMap.Store(ip, uid)
		l.aliveMu.RLock()
		aliveIp := l.AliveList[uid]
		l.aliveMu.RUnlock()
		// If any device is online
		if v, loaded := l.UserOnlineIP.LoadOrStore(taguuid, newipMap); loaded {
			oldipMap := v.(*sync.Map)
			// If this is a new ip
			if _, loaded := oldipMap.LoadOrStore(ip, uid); !loaded {
				if v, loaded := l.OldUserOnline.Load(ip); loaded {
					if v.(int) == uid {
						l.OldUserOnline.Delete(ip)
					}
				} else if deviceLimit > 0 {
					if deviceLimit <= aliveIp {
						oldipMap.Delete(ip)
						return nil, true
					}
				}
			}
		} else if v, ok := l.OldUserOnline.Load(ip); ok {
			if v.(int) == uid {
				l.OldUserOnline.Delete(ip)
			}
		} else {
			if deviceLimit > 0 {
				if deviceLimit <= aliveIp {
					l.UserOnlineIP.Delete(taguuid)
					return nil, true
				}
			}
		}
	}

	limit := int64(determineSpeedLimit(nodeLimit, userLimit)) * 1000000 / 8 // If you need the Speed limit
	// Creating and replacing buckets is serialized: Load→create→Store racing
	// across connections handed every racing connection its own bucket, so
	// the user's total rate limit was not shared. Existing connections keep
	// their old bucket pointer until their next CheckLimit call; every new
	// or re-checking connection immediately gets the current rate.
	l.bucketMu.Lock()
	defer l.bucketMu.Unlock()
	if limit > 0 {
		if v, ok := l.SpeedLimiter.Load(taguuid); ok {
			if old, isBucket := v.(*ratelimit.Bucket); isBucket && int64(old.Rate()) == limit {
				return old, false
			}
		}
		Bucket = ratelimit.NewBucketWithQuantum(time.Second, limit, limit) // Byte/s
		l.SpeedLimiter.Store(taguuid, Bucket)
		return Bucket, false
	}
	// Unlimited now: drop any stale bucket so a later limit change takes effect.
	l.SpeedLimiter.Delete(taguuid)
	return nil, false
}

func (l *Limiter) GetOnlineDevice() (*[]panel.OnlineUser, error) {
	var onlineUser []panel.OnlineUser
	l.OldUserOnline = new(sync.Map)
	l.UserOnlineIP.Range(func(key, value interface{}) bool {
		taguuid := key.(string)
		ipMap := value.(*sync.Map)
		ipMap.Range(func(key, value interface{}) bool {
			uid := value.(int)
			ip := key.(string)
			l.OldUserOnline.Store(ip, uid)
			onlineUser = append(onlineUser, panel.OnlineUser{UID: uid, IP: ip})
			return true
		})
		l.UserOnlineIP.Delete(taguuid) // Reset online device
		return true
	})

	return &onlineUser, nil
}

type UserIpList struct {
	Uid    int      `json:"Uid"`
	IpList []string `json:"Ips"`
}
