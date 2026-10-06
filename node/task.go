package node

import (
	"time"

	"github.com/InazumaV/V2bX/api/panel"
	"github.com/InazumaV/V2bX/common/task"
	vCore "github.com/InazumaV/V2bX/core"
	"github.com/InazumaV/V2bX/limiter"
	log "github.com/sirupsen/logrus"
)

func (c *Controller) startTasks(node *panel.NodeInfo) {
	// fetch node info task
	c.nodeInfoMonitorPeriodic = &task.Task{
		Interval: node.PullInterval,
		Execute:  c.nodeInfoMonitor,
	}
	// fetch user list task
	c.userReportPeriodic = &task.Task{
		Interval: node.PushInterval,
		Execute:  c.reportUserTrafficTask,
	}
	log.WithField("tag", c.tag).Info("Start monitor node status")
	// delay to start nodeInfoMonitor
	_ = c.nodeInfoMonitorPeriodic.Start(false)
	log.WithField("tag", c.tag).Info("Start report node status")
	_ = c.userReportPeriodic.Start(false)
	if node.Security == panel.Tls {
		switch c.CertConfig.CertMode {
		case "none", "", "file", "self":
		default:
			c.renewCertPeriodic = &task.Task{
				Interval: time.Hour * 24,
				Execute:  c.renewCertTask,
			}
			log.WithField("tag", c.tag).Info("Start renew cert")
			// delay to start renewCert
			_ = c.renewCertPeriodic.Start(true)
		}
	}
	if c.LimitConfig.EnableDynamicSpeedLimit {
		c.traffic = make(map[string]int64)
		c.dynamicSpeedLimitPeriodic = &task.Task{
			Interval: time.Duration(c.LimitConfig.DynamicSpeedLimitConfig.Periodic) * time.Second,
			Execute:  c.SpeedChecker,
		}
		if err := c.dynamicSpeedLimitPeriodic.Start(false); err != nil {
			log.Printf("[%s: %d] Start dynamic speed limit task failed: %s", c.apiClient.NodeType, c.apiClient.NodeId, err)
		} else {
			log.Printf("[%s: %d] Start dynamic speed limit", c.apiClient.NodeType, c.apiClient.NodeId)
		}
	}
}

func (c *Controller) nodeInfoMonitor() (err error) {
	// get node info
	newN, err := c.apiClient.GetNodeInfo()
	if err != nil {
		log.WithFields(log.Fields{
			"tag": c.tag,
			"err": err,
		}).Error("Get node info failed")
		return nil
	}
	// get user info
	newU, err := c.apiClient.GetUserList()
	if err != nil {
		log.WithFields(log.Fields{
			"tag": c.tag,
			"err": err,
		}).Error("Get user list failed")
		return nil
	}
	// get user alive
	newA, err := c.apiClient.GetUserAlive()
	if err != nil {
		log.WithFields(log.Fields{
			"tag": c.tag,
			"err": err,
		}).Error("Get alive list failed")
		return nil
	}
	if newN != nil {
		// nodeInfo changed — keep the previous state so a failed reload can
		// be rolled back instead of leaving the node down until the next
		// panel-side change.
		oldTag := c.tag
		oldInfo := c.info
		oldUserList := c.userList
		usersToApply := c.userList
		if newU != nil {
			// fetched list becomes the target; it is only recorded as applied
			// once the reload succeeded
			c.userListTarget = newU
			usersToApply = newU
		}
		c.info = newN
		c.resetTrafficCounter()
		// Remove old node
		log.WithField("tag", c.tag).Info("Node changed, reload")
		err = c.server.DelNode(c.tag)
		if err != nil {
			log.WithFields(log.Fields{
				"tag": c.tag,
				"err": err,
			}).Error("Delete node failed")
			c.info = oldInfo
			c.userList = oldUserList
			return nil
		}

		// Update limiter. Always rebuild it: with a custom node name the tag
		// never changes, so a stale limiter would keep rejecting newly added
		// users. DeleteLimiter must target the OLD tag, not the new one.
		if len(c.Options.Name) == 0 {
			limiter.DeleteLimiter(oldTag)
			c.tag = c.buildNodeTag(newN)
		}
		c.limiter = limiter.AddLimiter(c.tag, &c.LimitConfig, c.userList, newA)
		// Update rule
		err = c.limiter.UpdateRule(&newN.Rules)
		if err != nil {
			log.WithFields(log.Fields{
				"tag": c.tag,
				"err": err,
			}).Error("Update Rule failed")
			if rbErr := c.rollbackNodeReload(oldTag, oldInfo, oldUserList, false); rbErr != nil {
				log.WithFields(log.Fields{"tag": oldTag, "err": rbErr}).Error("Node rollback failed")
			}
			return nil
		}

		// check cert
		if newN.Security == panel.Tls {
			err = c.requestCert()
			if err != nil {
				log.WithFields(log.Fields{
					"tag": c.tag,
					"err": err,
				}).Error("Request cert failed")
				if rbErr := c.rollbackNodeReload(oldTag, oldInfo, oldUserList, false); rbErr != nil {
					log.WithFields(log.Fields{"tag": oldTag, "err": rbErr}).Error("Node rollback failed")
				}
				return nil
			}
		}
		// add new node
		err = c.server.AddNode(c.tag, newN, c.Options)
		if err != nil {
			log.WithFields(log.Fields{
				"tag": c.tag,
				"err": err,
			}).Error("Add node failed")
			// AddNode can fail halfway (e.g. inbound added, outbound not);
			// the rollback removes whatever was created for the new tag.
			if rbErr := c.rollbackNodeReload(oldTag, oldInfo, oldUserList, true); rbErr != nil {
				log.WithFields(log.Fields{"tag": oldTag, "err": rbErr}).Error("Node rollback failed")
			}
			return nil
		}
		_, err = c.server.AddUsers(&vCore.AddUsersParams{
			Tag:      c.tag,
			Users:    usersToApply,
			NodeInfo: newN,
		})
		if err != nil {
			log.WithFields(log.Fields{
				"tag": c.tag,
				"err": err,
			}).Error("Add users failed")
			if rbErr := c.rollbackNodeReload(oldTag, oldInfo, oldUserList, true); rbErr != nil {
				log.WithFields(log.Fields{"tag": oldTag, "err": rbErr}).Error("Node rollback failed")
			}
			// the fetched target stays pending and is retried via the user
			// diff on the next cycle
			return nil
		}
		c.userList = usersToApply
		c.userListTarget = nil
		// Check interval
		if c.nodeInfoMonitorPeriodic.Interval != newN.PullInterval &&
			newN.PullInterval != 0 {
			c.nodeInfoMonitorPeriodic.SetInterval(newN.PullInterval)
		}
		if c.userReportPeriodic.Interval != newN.PushInterval &&
			newN.PushInterval != 0 {
			c.userReportPeriodic.SetInterval(newN.PushInterval)
		}
		log.WithField("tag", c.tag).Infof("Added %d new users", len(c.userList))
		// exit
		return nil
	}
	// update alive list
	if newA != nil {
		c.limiter.SetAliveList(newA)
	}
	// A non-nil fetched list is the new target state; a 304 (nil) keeps any
	// target that has not been applied yet, so a failed apply is retried on
	// the following cycles instead of being lost until the next panel change.
	if newU != nil {
		c.userListTarget = newU
	}
	target := c.userListTarget
	if target == nil {
		// 304 and nothing pending: the applied user set stays valid
		return nil
	}
	deleted, added := compareUserList(c.userList, target)
	if len(deleted) > 0 {
		// have deleted users
		err = c.server.DelUsers(deleted, c.tag, c.info)
		if err != nil {
			log.WithFields(log.Fields{
				"tag": c.tag,
				"err": err,
			}).Error("Delete users failed")
			// target is kept pending; retried on the next cycle
			return nil
		}
		// record the deletion even if the adds below fail, so the next cycle
		// does not re-delete users the core no longer has
		c.userList = removeFromUserList(c.userList, deleted)
		// keep the limiter in sync incrementally
		c.limiter.UpdateUser(c.tag, nil, deleted)
		if c.LimitConfig.EnableDynamicSpeedLimit {
			c.trafficMu.Lock()
			for i := range deleted {
				delete(c.traffic, deleted[i].Uuid)
			}
			c.trafficMu.Unlock()
		}
	}
	if len(added) > 0 {
		// have added users
		_, err = c.server.AddUsers(&vCore.AddUsersParams{
			Tag:      c.tag,
			NodeInfo: c.info,
			Users:    added,
		})
		if err != nil {
			log.WithFields(log.Fields{
				"tag": c.tag,
				"err": err,
			}).Error("Add users failed")
			// the applied deletions are recorded; the pending adds are
			// retried on the next cycle
			return nil
		}
		c.userList = appendToUserList(c.userList, added)
		c.limiter.UpdateUser(c.tag, added, nil)
	}
	// target fully applied
	c.userListTarget = nil
	if len(added)+len(deleted) != 0 {
		log.WithField("tag", c.tag).
			Infof("%d user deleted, %d user added", len(deleted), len(added))
	}
	return nil
}

// removeFromUserList returns the list without the removed users (matched by
// Uuid+Id identity), recording partial apply progress across failed cycles.
func removeFromUserList(list []panel.UserInfo, removed []panel.UserInfo) []panel.UserInfo {
	gone := make(map[string]bool, len(removed))
	for i := range removed {
		gone[removed[i].Uuid] = true
	}
	out := make([]panel.UserInfo, 0, len(list))
	for _, u := range list {
		if !gone[u.Uuid] {
			out = append(out, u)
		}
	}
	return out
}

// appendToUserList returns the list plus the added users.
func appendToUserList(list []panel.UserInfo, added []panel.UserInfo) []panel.UserInfo {
	out := make([]panel.UserInfo, 0, len(list)+len(added))
	out = append(out, list...)
	out = append(out, added...)
	return out
}

// rollbackNodeReload restores the previous node after a failed reload so it
// keeps serving instead of staying down until the next panel-side change.
// newNodeInCore tells whether the new node (or parts of it) already made it
// into the core; those resources are removed first — otherwise re-adding the
// old node fails with a duplicate inbound when the tags match, or leaves the
// new node behind when they differ. An error return means the core is NOT
// guaranteed to match the controller state.
func (c *Controller) rollbackNodeReload(oldTag string, oldInfo *panel.NodeInfo, oldUserList []panel.UserInfo, newNodeInCore bool) error {
	newTag := c.tag
	if newNodeInCore {
		if err := c.server.DelNode(newTag); err != nil {
			log.WithFields(log.Fields{
				"tag": newTag,
				"err": err,
			}).Warn("Rollback: remove failed new node")
		}
	}
	// Drop the limiter created for the new configuration; when the tags are
	// equal it is re-created for the old configuration below.
	limiter.DeleteLimiter(newTag)

	c.tag = oldTag
	c.info = oldInfo
	c.userList = oldUserList
	c.limiter = limiter.AddLimiter(oldTag, &c.LimitConfig, oldUserList, nil)
	if err := c.limiter.UpdateRule(&oldInfo.Rules); err != nil {
		log.WithFields(log.Fields{
			"tag": oldTag,
			"err": err,
		}).Error("Rollback: restore rules failed")
	}
	if err := c.server.AddNode(oldTag, oldInfo, c.Options); err != nil {
		log.WithFields(log.Fields{
			"tag": oldTag,
			"err": err,
		}).Error("Rollback failed: old node could not be restored")
		return err
	}
	if _, err := c.server.AddUsers(&vCore.AddUsersParams{
		Tag:      oldTag,
		Users:    oldUserList,
		NodeInfo: oldInfo,
	}); err != nil {
		log.WithFields(log.Fields{
			"tag": oldTag,
			"err": err,
		}).Error("Rollback failed: old users could not be restored")
		return err
	}
	return nil
}

func (c *Controller) SpeedChecker() error {
	if c.traffic == nil {
		return nil
	}
	// Snapshot and clear expired entries under the lock.
	c.trafficMu.Lock()
	over := make([]string, 0)
	for u, t := range c.traffic {
		if t >= c.LimitConfig.DynamicSpeedLimitConfig.Traffic {
			over = append(over, u)
			delete(c.traffic, u)
		}
	}
	c.trafficMu.Unlock()
	if len(over) == 0 {
		return nil
	}
	// Fetch the limiter through the global registry instead of c.limiter:
	// the node monitor swaps that field during a reload while this task runs.
	l, err := limiter.GetLimiter(c.tag)
	if err != nil {
		log.WithFields(log.Fields{
			"tag": c.tag,
			"err": err,
		}).Error("Get limiter for dynamic speed limit failed")
		return nil
	}
	for _, u := range over {
		if err := l.UpdateDynamicSpeedLimit(c.tag, u,
			c.LimitConfig.DynamicSpeedLimitConfig.SpeedLimit,
			time.Now().Add(time.Duration(c.LimitConfig.DynamicSpeedLimitConfig.ExpireTime)*time.Minute)); err != nil {
			log.WithFields(log.Fields{
				"tag": c.tag,
				"err": err,
			}).Error("Update dynamic speed limit failed")
		}
	}
	return nil
}
