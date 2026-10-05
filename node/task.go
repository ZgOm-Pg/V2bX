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
		log.Printf("[%s: %d] Start dynamic speed limit", c.apiClient.NodeType, c.apiClient.NodeId)
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
		c.info = newN
		if newU != nil {
			c.userList = newU
		}
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
			c.rollbackNodeReload(oldTag, oldInfo, oldUserList)
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
				c.rollbackNodeReload(oldTag, oldInfo, oldUserList)
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
			c.rollbackNodeReload(oldTag, oldInfo, oldUserList)
			return nil
		}
		_, err = c.server.AddUsers(&vCore.AddUsersParams{
			Tag:      c.tag,
			Users:    c.userList,
			NodeInfo: newN,
		})
		if err != nil {
			log.WithFields(log.Fields{
				"tag": c.tag,
				"err": err,
			}).Error("Add users failed")
			c.rollbackNodeReload(oldTag, oldInfo, oldUserList)
			return nil
		}
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
	// node no changed, check users
	if len(newU) == 0 {
		return nil
	}
	deleted, added := compareUserList(c.userList, newU)
	if len(deleted) > 0 {
		// have deleted users
		err = c.server.DelUsers(deleted, c.tag, c.info)
		if err != nil {
			log.WithFields(log.Fields{
				"tag": c.tag,
				"err": err,
			}).Error("Delete users failed")
			return nil
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
			return nil
		}
	}
	if len(added) > 0 || len(deleted) > 0 {
		// update Limiter
		c.limiter.UpdateUser(c.tag, added, deleted)
		if err != nil {
			log.WithFields(log.Fields{
				"tag": c.tag,
				"err": err,
			}).Error("limiter users failed")
			return nil
		}
		// clear traffic record
		if c.LimitConfig.EnableDynamicSpeedLimit {
			c.trafficMu.Lock()
			for i := range deleted {
				delete(c.traffic, deleted[i].Uuid)
			}
			c.trafficMu.Unlock()
		}
	}
	c.userList = newU
	if len(added)+len(deleted) != 0 {
		log.WithField("tag", c.tag).
			Infof("%d user deleted, %d user added", len(deleted), len(added))
	}
	return nil
}

// rollbackNodeReload restores the previous node after a failed reload so it
// keeps serving instead of staying down until the next panel-side change.
func (c *Controller) rollbackNodeReload(oldTag string, oldInfo *panel.NodeInfo, oldUserList []panel.UserInfo) {
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
		}).Error("Rollback: add old node failed")
		return
	}
	if _, err := c.server.AddUsers(&vCore.AddUsersParams{
		Tag:      oldTag,
		Users:    oldUserList,
		NodeInfo: oldInfo,
	}); err != nil {
		log.WithFields(log.Fields{
			"tag": oldTag,
			"err": err,
		}).Error("Rollback: add old users failed")
	}
}

func (c *Controller) SpeedChecker() error {
	if c.traffic == nil {
		return nil
	}
	// Snapshot and clear expired entries under the lock; the limiter update
	// itself does network-free map lookups but keep it out of the lock anyway.
	c.trafficMu.Lock()
	over := make([]string, 0)
	for u, t := range c.traffic {
		if t >= c.LimitConfig.DynamicSpeedLimitConfig.Traffic {
			over = append(over, u)
			delete(c.traffic, u)
		}
	}
	c.trafficMu.Unlock()
	for _, u := range over {
		if err := c.limiter.UpdateDynamicSpeedLimit(c.tag, u,
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
