package main

import (
	"strings"
	"testing"
)

func TestPublishedConfigurationFormats(t *testing.T) {
	for _, sample := range []struct {
		name, protocol string
		settings       map[string]any
		network        string
	}{
		{"clash_vless", "vless", map[string]any{"type": "vless", "uuid": "fbd61e7f-0960-4fe4-9a65-7b87d00e43e7"}, ""},
		{"string_boolean", "vmess", map[string]any{"uuid": "fbd61e7f-0960-4fe4-9a65-7b87d00e43e7", "udp": "true", "tls": "false", "net": "tcp"}, ""},
		{"vmess_ws", "vmess", map[string]any{"id": "fbd61e7f-0960-4fe4-9a65-7b87d00e43e7", "net": "ws", "path": "/test", "host": "example.com", "tls": "tls"}, "ws"},
		{"uri_upgrade", "vless", map[string]any{"uuid": "fbd61e7f-0960-4fe4-9a65-7b87d00e43e7", "type": "httpupgrade", "path": "/test"}, "ws"},
		{"singbox_trojan", "trojan", map[string]any{"type": "trojan", "password": "test", "tls": map[string]any{"enabled": true, "server_name": "example.com"}, "transport": map[string]any{"type": "ws", "path": "/test"}}, "ws"},
		{"xray_ss", "shadowsocks", map[string]any{"endpoint": map[string]any{"method": "aes-128-gcm", "password": "test"}}, ""},
	} {
		t.Run(sample.name, func(t *testing.T) {
			c := Candidate{ID: 1, Key: strings.Repeat("a", 64), Address: "example.com", Port: 443, Protocol: sample.protocol, Settings: sample.settings}
			m, err := normalize(c, c.Protocol)
			if err != nil || firstString(m, "network") != sample.network {
				t.Fatalf("normalization failed: %v", err)
			}
			if code := validateConfiguration(c); code != "valid" {
				t.Fatalf("adapter rejected format: %s", code)
			}
		})
	}
}

func TestPrivateDestinationFiltering(t *testing.T) {
	for _, address := range []string{"127.0.0.1", "10.2.3.4", "169.254.169.254", "100.64.0.1", "::1", "fd00::1"} {
		if publicAddress(mustAddress(address)) {
			t.Errorf("private address accepted: %s", address)
		}
	}
}
