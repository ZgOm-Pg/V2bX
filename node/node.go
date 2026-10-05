package node

import (
	"github.com/InazumaV/V2bX/api/panel"
	"github.com/InazumaV/V2bX/conf"
	vCore "github.com/InazumaV/V2bX/core"
	log "github.com/sirupsen/logrus"
)

type Node struct {
	controllers []*Controller
}

func New() *Node {
	return &Node{}
}

func (n *Node) Start(nodes []conf.NodeConfig, core vCore.Core) error {
	n.controllers = make([]*Controller, len(nodes))
	for i := range nodes {
		p, err := panel.New(&nodes[i].ApiConfig)
		if err != nil {
			// One misconfigured node must not take the other nodes on the
			// instance down with it.
			log.WithFields(log.Fields{
				"api_host": nodes[i].ApiConfig.APIHost,
				"type":     nodes[i].ApiConfig.NodeType,
				"id":       nodes[i].ApiConfig.NodeID,
				"err":      err,
			}).Error("Create panel client failed, skip this node")
			n.controllers[i] = nil
			continue
		}
		// Register controller service
		n.controllers[i] = NewController(core, p, &nodes[i].Options)
		err = n.controllers[i].Start()
		if err != nil {
			log.WithFields(log.Fields{
				"api_host": nodes[i].ApiConfig.APIHost,
				"type":     nodes[i].ApiConfig.NodeType,
				"id":       nodes[i].ApiConfig.NodeID,
				"err":      err,
			}).Error("Start node controller failed, skip this node")
			n.controllers[i] = nil
			continue
		}
	}
	return nil
}

func (n *Node) Close() {
	for _, c := range n.controllers {
		if c == nil {
			continue
		}
		if err := c.Close(); err != nil {
			log.WithField("err", err).Error("Close node controller failed")
		}
	}
	n.controllers = nil
}
