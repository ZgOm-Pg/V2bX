package sing

import (
	"encoding/base64"
	"errors"

	"github.com/InazumaV/V2bX/api/panel"
	"github.com/InazumaV/V2bX/common/counter"
	"github.com/InazumaV/V2bX/core"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing-box/protocol/anytls"
	"github.com/sagernet/sing-box/protocol/hysteria"
	"github.com/sagernet/sing-box/protocol/hysteria2"
	"github.com/sagernet/sing-box/protocol/shadowsocks"
	"github.com/sagernet/sing-box/protocol/trojan"
	"github.com/sagernet/sing-box/protocol/tuic"
	"github.com/sagernet/sing-box/protocol/vless"
	"github.com/sagernet/sing-box/protocol/vmess"
)

func (b *Sing) AddUsers(p *core.AddUsersParams) (added int, err error) {
	in, found := b.box.Inbound().Get(p.Tag)
	if !found {
		return 0, errors.New("the inbound not found")
	}
	b.users.mapLock.Lock()
	defer b.users.mapLock.Unlock()
	for i := range p.Users {
		b.users.uidMap[p.Users[i].Uuid] = p.Users[i].Id
	}
	switch p.NodeInfo.Type {
	case "vless":
		us := make([]option.VLESSUser, len(p.Users))
		for i := range p.Users {
			us[i] = option.VLESSUser{
				Name: p.Users[i].Uuid,
				Flow: p.VAllss.Flow,
				UUID: p.Users[i].Uuid,
			}
		}
		err = in.(*vless.Inbound).AddUsers(us)
	case "vmess":
		us := make([]option.VMessUser, len(p.Users))
		for i := range p.Users {
			us[i] = option.VMessUser{
				Name: p.Users[i].Uuid,
				UUID: p.Users[i].Uuid,
			}
		}
		err = in.(*vmess.Inbound).AddUsers(us)
	case "shadowsocks":
		us := make([]option.ShadowsocksUser, len(p.Users))
		for i := range p.Users {
			var password = p.Users[i].Uuid
			switch p.Shadowsocks.Cipher {
			case "2022-blake3-aes-128-gcm":
				password = base64.StdEncoding.EncodeToString([]byte(password[:16]))
			case "2022-blake3-aes-256-gcm":
				password = base64.StdEncoding.EncodeToString([]byte(password[:32]))
			}
			us[i] = option.ShadowsocksUser{
				Name:     p.Users[i].Uuid,
				Password: password,
			}
		}
		err = in.(*shadowsocks.MultiInbound).AddUsers(us)
	case "trojan":
		us := make([]option.TrojanUser, len(p.Users))
		for i := range p.Users {
			us[i] = option.TrojanUser{
				Name:     p.Users[i].Uuid,
				Password: p.Users[i].Uuid,
			}
		}
		err = in.(*trojan.Inbound).AddUsers(us)
	case "tuic":
		us := make([]option.TUICUser, len(p.Users))
		id := make([]int, len(p.Users))
		for i := range p.Users {
			us[i] = option.TUICUser{
				Name:     p.Users[i].Uuid,
				UUID:     p.Users[i].Uuid,
				Password: p.Users[i].Uuid,
			}
			id[i] = p.Users[i].Id
		}
		err = in.(*tuic.Inbound).AddUsers(us, id)
	case "hysteria":
		us := make([]option.HysteriaUser, len(p.Users))
		for i := range p.Users {
			us[i] = option.HysteriaUser{
				Name:       p.Users[i].Uuid,
				AuthString: p.Users[i].Uuid,
			}
		}
		err = in.(*hysteria.Inbound).AddUsers(us)
	case "hysteria2":
		us := make([]option.Hysteria2User, len(p.Users))
		id := make([]int, len(p.Users))
		for i := range p.Users {
			us[i] = option.Hysteria2User{
				Name:     p.Users[i].Uuid,
				Password: p.Users[i].Uuid,
			}
			id[i] = p.Users[i].Id
		}
		err = in.(*hysteria2.Inbound).AddUsers(us, id)
	case "anytls":
		us := make([]option.AnyTLSUser, len(p.Users))
		for i := range p.Users {
			us[i] = option.AnyTLSUser{
				Name:     p.Users[i].Uuid,
				Password: p.Users[i].Uuid,
			}
		}
		err = in.(*anytls.Inbound).AddUsers(us)
	}
	if err != nil {
		return 0, err
	}
	return len(p.Users), err
}

func (b *Sing) GetUserTraffic(tag, uuid string, reset bool) (up int64, down int64) {
	if v, ok := b.hookServer.counter.Load(tag); ok {
		c := v.(*counter.TrafficCounter)
		up = c.GetUpCount(uuid)
		down = c.GetDownCount(uuid)
		if reset {
			c.Reset(uuid)
		}
		return
	}
	return 0, 0
}

func (b *Sing) GetUserTrafficSlice(tag string, reset bool) ([]panel.UserTraffic, error) {
	trafficSlice := make([]panel.UserTraffic, 0)
	hook := b.hookServer
	b.users.mapLock.RLock()
	defer b.users.mapLock.RUnlock()
	if v, ok := hook.counter.Load(tag); ok {
		c := v.(*counter.TrafficCounter)
		minTraffic := b.getMinTraffic(tag)
		c.Counters.Range(func(key, value interface{}) bool {
			uuid := key.(string)
			traffic := value.(*counter.TrafficStorage)
			// Swap(0) reads and clears atomically; a plain Load followed by
			// Store(0) silently erased everything added in between.
			up := traffic.UpCounter.Swap(0)
			down := traffic.DownCounter.Swap(0)
			if b.users.uidMap[uuid] == 0 {
				// leftover counter of a removed user: drop it with its bytes
				c.Delete(uuid)
				return true
			}
			if up+down > minTraffic {
				if !reset {
					traffic.UpCounter.Add(up)
					traffic.DownCounter.Add(down)
				}
				trafficSlice = append(trafficSlice, panel.UserTraffic{
					UID:      b.users.uidMap[uuid],
					Upload:   up,
					Download: down,
				})
			} else {
				// below the reporting threshold: put the bytes back so they
				// keep accumulating for a later cycle
				traffic.UpCounter.Add(up)
				traffic.DownCounter.Add(down)
			}
			return true
		})
		if len(trafficSlice) == 0 {
			return nil, nil
		}
		return trafficSlice, nil
	}
	return nil, nil
}

// RestoreUserTraffic puts drained-but-unacknowledged traffic back into the
// counters after a failed report, so the bytes are not lost for the next cycle.
func (b *Sing) RestoreUserTraffic(tag string, traffic []panel.UserTraffic) error {
	b.users.mapLock.RLock()
	defer b.users.mapLock.RUnlock()
	v, ok := b.hookServer.counter.Load(tag)
	if !ok {
		return nil
	}
	c := v.(*counter.TrafficCounter)
	for i := range traffic {
		for uuid, uid := range b.users.uidMap {
			if uid != traffic[i].UID {
				continue
			}
			cts := c.GetCounter(uuid)
			cts.UpCounter.Add(traffic[i].Upload)
			cts.DownCounter.Add(traffic[i].Download)
		}
	}
	return nil
}

type UserDeleter interface {
	DelUsers(uuid []string) error
}

func (b *Sing) DelUsers(users []panel.UserInfo, tag string, info *panel.NodeInfo) error {
	var del UserDeleter
	if i, found := b.box.Inbound().Get(tag); found {
		switch info.Type {
		case "vmess":
			del = i.(*vmess.Inbound)
		case "vless":
			del = i.(*vless.Inbound)
		case "shadowsocks":
			del = i.(*shadowsocks.MultiInbound)
		case "trojan":
			del = i.(*trojan.Inbound)
		case "tuic":
			del = i.(*tuic.Inbound)
		case "hysteria":
			del = i.(*hysteria.Inbound)
		case "hysteria2":
			del = i.(*hysteria2.Inbound)
		case "anytls":
			del = i.(*anytls.Inbound)
		}
	} else {
		return errors.New("the inbound not found")
	}
	uuids := make([]string, len(users))
	b.users.mapLock.Lock()
	defer b.users.mapLock.Unlock()
	for i := range users {
		if v, ok := b.hookServer.counter.Load(tag); ok {
			c := v.(*counter.TrafficCounter)
			c.Delete(users[i].Uuid)
		}
		delete(b.users.uidMap, users[i].Uuid)
		uuids[i] = users[i].Uuid
	}
	err := del.DelUsers(uuids)
	if err != nil {
		return err
	}
	return nil
}
