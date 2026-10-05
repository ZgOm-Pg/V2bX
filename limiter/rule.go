package limiter

import (
	"regexp"

	"github.com/InazumaV/V2bX/api/panel"
	log "github.com/sirupsen/logrus"
)

func (l *Limiter) CheckDomainRule(destination string) (reject bool) {
	// have rule
	for i := range l.DomainRules {
		if l.DomainRules[i].MatchString(destination) {
			reject = true
			break
		}
	}
	return
}

func (l *Limiter) CheckProtocolRule(protocol string) (reject bool) {
	for i := range l.ProtocolRules {
		if l.ProtocolRules[i] == protocol {
			reject = true
			break
		}
	}
	return
}

func (l *Limiter) UpdateRule(rule *panel.Rules) error {
	// Compile instead of MustCompile: panel-controlled rules must not be able
	// to panic (and with no recover anywhere, crash) the process.
	rules := make([]*regexp.Regexp, 0, len(rule.Regexp))
	for i := range rule.Regexp {
		re, err := regexp.Compile(rule.Regexp[i])
		if err != nil {
			log.WithFields(log.Fields{
				"rule": rule.Regexp[i],
				"err":  err,
			}).Error("invalid regexp rule from panel, skipped")
			continue
		}
		rules = append(rules, re)
	}
	l.DomainRules = rules
	l.ProtocolRules = rule.Protocol
	return nil
}
