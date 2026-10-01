package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/netip"
	"strconv"
	"strings"
)

type Candidate struct {
	ID              int64          `json:"id"`
	Key             string         `json:"key"`
	Address         string         `json:"address"`
	Port            int            `json:"port"`
	Protocol        string         `json:"protocol"`
	WorkingProtocol string         `json:"working_protocol,omitempty"`
	Settings        map[string]any `json:"settings"`
}

type configError string

func (e configError) Error() string { return string(e) }

func stringValue(value any) string {
	switch v := value.(type) {
	case string:
		return v
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case int:
		return strconv.Itoa(v)
	case bool:
		return strconv.FormatBool(v)
	}
	return ""
}

func firstString(m map[string]any, keys ...string) string {
	for _, k := range keys {
		if v := stringValue(m[k]); v != "" {
			return v
		}
	}
	return ""
}

func boolean(value any) bool {
	switch strings.ToLower(stringValue(value)) {
	case "true", "1", "yes", "tls", "reality", "xtls":
		return true
	}
	return false
}

func object(value any) map[string]any {
	if value, ok := value.(map[string]any); ok {
		return value
	}
	if value, ok := value.(string); ok {
		var parsed map[string]any
		if json.Unmarshal([]byte(value), &parsed) == nil && parsed != nil {
			return parsed
		}
	}
	return map[string]any{}
}

func stringList(value any) []string {
	var result []string
	switch v := value.(type) {
	case string:
		for _, part := range strings.Split(v, ",") {
			if part = strings.TrimSpace(part); part != "" {
				result = append(result, part)
			}
		}
	case []any:
		for _, part := range v {
			if value := stringValue(part); value != "" {
				result = append(result, value)
			}
		}
	case []string:
		return v
	}
	return result
}

func integer(value any, fallback int) int {
	n, err := strconv.Atoi(stringValue(value))
	if err != nil {
		return fallback
	}
	return n
}

func decodeOptionalBase64(value any) string {
	s := stringValue(value)
	for _, encoding := range []*base64.Encoding{base64.RawURLEncoding, base64.URLEncoding, base64.RawStdEncoding, base64.StdEncoding} {
		if decoded, err := encoding.DecodeString(s); err == nil {
			return string(decoded)
		}
	}
	return s
}

func normalize(c Candidate, protocol string) (map[string]any, error) {
	if c.Address == "" || c.Port < 1 || c.Port > 65535 {
		return nil, configError("invalid_endpoint")
	}
	s := c.Settings
	if boolean(s["_invalid_settings"]) {
		return nil, configError("invalid_settings")
	}
	if protocol == "mtproto" {
		return nil, configError("telegram_only_protocol")
	}
	if protocol == "warp" {
		return nil, configError("warp_missing_wireguard_keys")
	}
	allowed := map[string]bool{"http": true, "https": true, "socks4": true, "socks5": true,
		"vmess": true, "vless": true, "trojan": true, "shadowsocks": true, "shadowsocksr": true,
		"hysteria": true, "hysteria2": true, "tuic": true, "wireguard": true, "anytls": true}
	if !allowed[protocol] {
		return nil, configError("unsupported_protocol")
	}
	typeName := protocol
	if protocol == "https" {
		typeName = "http"
	} else if protocol == "shadowsocks" {
		typeName = "ss"
	} else if protocol == "shadowsocksr" {
		typeName = "ssr"
	}
	m := map[string]any{"name": fmt.Sprintf("proxy-%d", c.ID), "type": typeName, "server": c.Address, "port": c.Port}
	// Only transport fields are admitted; published routing, interface and file options are not executed.
	for _, k := range strings.Fields("username password uuid cipher flow packet-encoding packet-addr xudp udp ws-opts grpc-opts http-opts h2-opts xhttp-opts reality-opts client-fingerprint fingerprint plugin plugin-opts obfs obfs-password obfs-param protocol-param up down ip ipv6 private-key public-key pre-shared-key reserved mtu persistent-keepalive token reduce-rtt udp-relay-mode congestion-controller disable-sni request-timeout") {
		if value, ok := s[k]; ok {
			m[k] = value
		}
	}
	for _, k := range strings.Fields("packet-addr xudp udp reduce-rtt disable-sni") {
		if value, ok := m[k]; ok {
			m[k] = boolean(value)
		}
	}
	for _, k := range strings.Fields("ws-opts grpc-opts http-opts h2-opts xhttp-opts reality-opts plugin-opts") {
		if value, ok := m[k]; ok {
			m[k] = object(value)
		}
	}
	m["username"] = firstString(s, "username", "user")
	if value := firstString(s, "password", "pass"); value != "" {
		m["password"] = value
	}
	user, endpoint := object(s["user"]), object(s["endpoint"])
	for _, k := range []string{"password", "uuid", "flow", "cipher"} {
		if firstString(m, k) == "" {
			if value := firstString(user, k); value != "" {
				m[k] = value
			} else if value := firstString(endpoint, k); value != "" {
				m[k] = value
			}
		}
	}
	if firstString(m, "cipher") == "" {
		m["cipher"] = firstString(s, "method")
		if firstString(m, "cipher") == "" {
			m["cipher"] = firstString(endpoint, "method")
		}
	}
	if firstString(m, "uuid") == "" {
		m["uuid"] = firstString(s, "uuid", "id")
		if firstString(m, "uuid") == "" {
			m["uuid"] = firstString(user, "id")
		}
	}
	stream := object(s["streamSettings"])
	security := firstString(s, "security")
	if security == "" {
		security = firstString(stream, "security")
	}
	tlsMap := object(s["tls"])
	xTLS := object(stream["tlsSettings"])
	if security == "reality" {
		xTLS = object(stream["realitySettings"])
	}
	m["tls"] = protocol == "https" || protocol == "trojan" || boolean(s["tls"]) || boolean(tlsMap["enabled"]) || boolean(security)
	m["skip-cert-verify"] = boolean(s["skip-cert-verify"]) || boolean(s["insecure"]) || boolean(s["allowInsecure"]) || boolean(s["allow_insecure"]) || boolean(s["allowinsecure"]) || boolean(tlsMap["insecure"]) || boolean(xTLS["allowInsecure"])
	serverName := firstString(s, "servername", "sni", "peer")
	if serverName == "" {
		serverName = firstString(tlsMap, "server_name")
	}
	if serverName == "" {
		serverName = firstString(xTLS, "serverName")
	}
	if serverName != "" {
		m["servername"], m["sni"] = serverName, serverName
	}
	for _, value := range []any{s["alpn"], tlsMap["alpn"], xTLS["alpn"]} {
		if parts := stringList(value); len(parts) > 0 {
			m["alpn"] = parts
			break
		}
	}
	if firstString(m, "client-fingerprint") == "" {
		if value := firstString(s, "fp"); value != "" {
			m["client-fingerprint"] = value
		} else if value := firstString(xTLS, "fingerprint"); value != "" {
			m["client-fingerprint"] = value
		} else if value := firstString(object(tlsMap["utls"]), "fingerprint"); value != "" {
			m["client-fingerprint"] = value
		}
	}
	singReality := object(tlsMap["reality"])
	if security == "reality" || boolean(singReality["enabled"]) {
		m["tls"] = true
		reality := object(m["reality-opts"])
		if len(reality) == 0 {
			publicKey := firstString(s, "pbk", "public-key")
			if publicKey == "" {
				publicKey = firstString(xTLS, "publicKey")
			}
			if publicKey == "" {
				publicKey = firstString(singReality, "public_key")
			}
			shortID := firstString(s, "sid", "short-id")
			if shortID == "" {
				shortID = firstString(xTLS, "shortId")
			}
			if shortID == "" {
				shortID = firstString(singReality, "short_id")
			}
			reality = map[string]any{"public-key": publicKey, "short-id": shortID}
		}
		if firstString(reality, "public-key") == "" {
			return nil, configError("missing_reality_public_key")
		}
		m["reality-opts"] = reality
		if firstString(m, "client-fingerprint") == "" {
			m["client-fingerprint"] = "chrome"
		}
	}
	network := firstString(s, "network", "net")
	if network == "" {
		network = firstString(stream, "network")
	}
	if network == "" && protocol != "vmess" {
		network = firstString(s, "type")
		if allowed[network] || network == "ss" || network == "ssr" {
			network = ""
		}
	}
	if network == "" {
		network = firstString(object(s["transport"]), "type")
	}
	if network == "" && boolean(s["ws"]) {
		network = "ws"
	}
	if network == "splithttp" {
		network = "xhttp"
	}
	if network == "raw" || network == "tcp" || network == "none" {
		network = ""
	}
	if protocol == "vmess" || protocol == "vless" || protocol == "trojan" {
		if !map[string]bool{"": true, "ws": true, "grpc": true, "h2": true, "http": true, "xhttp": true, "httpupgrade": true, "kcp": true}[network] {
			return nil, configError("unsupported_transport")
		}
		if network == "httpupgrade" {
			network = "ws"
			ws := object(m["ws-opts"])
			ws["v2ray-http-upgrade"] = true
			m["ws-opts"] = ws
		}
		if network == "kcp" {
			if protocol != "vmess" {
				return nil, configError("unsupported_kcp_transport")
			}
			m["mkcp-opts"] = object(stream["kcpSettings"])
		}
		m["network"] = network
		path := firstString(s, "path", "ws-path", "wspath")
		host := firstString(s, "host", "Host")
		if network == "ws" {
			ws := object(m["ws-opts"])
			xws, transport := object(stream["wsSettings"]), object(s["transport"])
			if path == "" {
				path = firstString(xws, "path")
			}
			if path == "" {
				path = firstString(transport, "path")
			}
			if firstString(ws, "path") == "" && path != "" {
				ws["path"] = path
			}
			if len(object(ws["headers"])) == 0 {
				if headers := object(xws["headers"]); len(headers) > 0 {
					ws["headers"] = headers
				} else if host != "" {
					ws["headers"] = map[string]any{"Host": host}
				} else if headers := object(transport["headers"]); len(headers) > 0 {
					ws["headers"] = headers
				}
			}
			if value := firstString(s, "ed", "max_early_data"); value != "" {
				ws["max-early-data"] = integer(value, 0)
			}
			if value := firstString(s, "eh", "early_data_header_name"); value != "" {
				ws["early-data-header-name"] = value
			}
			m["ws-opts"] = ws
		} else if network == "grpc" {
			grpc := object(m["grpc-opts"])
			if firstString(grpc, "grpc-service-name") == "" {
				value := firstString(s, "serviceName", "service_name")
				if value == "" {
					value = firstString(object(stream["grpcSettings"]), "serviceName")
				}
				if value == "" {
					value = path
				}
				grpc["grpc-service-name"] = value
			}
			m["grpc-opts"] = grpc
		} else if network == "xhttp" {
			xhttp := object(m["xhttp-opts"])
			if len(xhttp) == 0 {
				xhttp = object(stream["xhttpSettings"])
			}
			if path != "" {
				xhttp["path"] = path
			}
			if host != "" {
				xhttp["host"] = host
			}
			if value := firstString(s, "mode"); value != "" {
				xhttp["mode"] = value
			}
			m["xhttp-opts"] = xhttp
		} else if network == "http" || (network == "" && (firstString(s, "headerType") == "http" || protocol == "vmess" && firstString(s, "type") == "http")) {
			m["network"] = "http"
			httpOpts := object(m["http-opts"])
			if path != "" {
				httpOpts["path"] = []string{path}
			}
			if host != "" {
				httpOpts["headers"] = map[string]any{"Host": []string{host}}
			}
			m["http-opts"] = httpOpts
		} else if network == "h2" {
			h2 := object(m["h2-opts"])
			if path != "" {
				h2["path"] = path
			}
			if host != "" {
				h2["host"] = []string{host}
			}
			m["h2-opts"] = h2
		}
	}
	switch protocol {
	case "vmess":
		m["alterId"] = integer(s["alterId"], integer(s["aid"], integer(user["alterId"], 0)))
		if firstString(m, "cipher") == "" {
			m["cipher"] = firstString(s, "scy")
		}
		if firstString(m, "cipher") == "" {
			m["cipher"] = "auto"
		}
		fallthrough
	case "vless", "tuic":
		if firstString(m, "uuid") == "" {
			return nil, configError("missing_uuid")
		}
		if protocol == "vless" {
			m["encryption"] = firstString(s, "encryption")
		}
		if protocol == "tuic" {
			m["congestion-controller"] = firstString(s, "congestion-controller", "congestion_controller", "congestion_control")
			m["udp-relay-mode"] = firstString(s, "udp-relay-mode", "udp_relay_mode")
			m["disable-sni"] = boolean(s["disable-sni"]) || boolean(s["disable_sni"])
		}
	case "shadowsocks", "shadowsocksr":
		if firstString(m, "cipher") == "" || firstString(m, "password") == "" {
			return nil, configError("missing_cipher_or_password")
		}
		if protocol == "shadowsocksr" {
			m["protocol"] = firstString(s, "ssr_protocol", "protocol")
			m["obfs-param"] = decodeOptionalBase64(s["obfsparam"])
			m["protocol-param"] = decodeOptionalBase64(s["protoparam"])
			if firstString(m, "protocol") == "" {
				return nil, configError("missing_ssr_protocol")
			}
		}
		if plugin := firstString(s, "plugin"); strings.Contains(plugin, ";") {
			parts := strings.Split(plugin, ";")
			m["plugin"] = parts[0]
			pluginOpts := object(m["plugin-opts"])
			for _, part := range parts[1:] {
				key, value, hasValue := strings.Cut(part, "=")
				if hasValue {
					pluginOpts[key] = value
				} else {
					pluginOpts[key] = true
				}
			}
			m["plugin-opts"] = pluginOpts
		}
		if firstString(m, "plugin") == "obfs-local" {
			m["plugin"] = "obfs"
			opts := object(m["plugin-opts"])
			if value := firstString(opts, "obfs"); value != "" {
				opts["mode"] = value
			}
			if value := firstString(opts, "obfs-host"); value != "" {
				opts["host"] = value
			}
			m["plugin-opts"] = opts
		}
	case "trojan", "anytls":
		if firstString(m, "password") == "" {
			return nil, configError("missing_password")
		}
	case "hysteria":
		m["auth-str"] = firstString(s, "auth-str", "auth_str", "auth", "password")
		m["up"] = firstString(s, "up", "upmbps")
		m["down"] = firstString(s, "down", "downmbps")
		if firstString(m, "up") == "" {
			m["up"] = "100"
		}
		if firstString(m, "down") == "" {
			m["down"] = "100"
		}
		if value := firstString(s, "protocol"); value != "" {
			m["protocol"] = value
		}
	case "hysteria2":
		if firstString(m, "password") == "" {
			m["password"] = firstString(s, "auth")
		}
	case "wireguard":
		m["private-key"] = firstString(s, "private-key", "private_key", "username")
		m["public-key"] = firstString(s, "public-key", "publickey", "publick")
		if value := firstString(s, "presharedkey"); value != "" {
			m["pre-shared-key"] = value
		}
		for _, address := range stringList(s["address"]) {
			ip, err := netip.ParsePrefix(address)
			if err != nil {
				continue
			}
			if ip.Addr().Is4() {
				m["ip"] = ip.Addr().String()
			} else {
				m["ipv6"] = ip.Addr().String()
			}
		}
		if firstString(m, "private-key") == "" || firstString(m, "public-key") == "" {
			return nil, configError("missing_wireguard_keys")
		}
		if firstString(m, "ip") == "" && firstString(m, "ipv6") == "" {
			return nil, configError("missing_wireguard_address")
		}
		if value, ok := m["reserved"].(string); ok {
			if decoded, err := base64.StdEncoding.DecodeString(value); err == nil && len(decoded) == 3 {
				m["reserved"] = []int{int(decoded[0]), int(decoded[1]), int(decoded[2])}
			} else {
				parts := strings.Split(value, ",")
				if len(parts) != 3 {
					return nil, configError("invalid_wireguard_reserved")
				}
				m["reserved"] = []int{integer(parts[0], -1), integer(parts[1], -1), integer(parts[2], -1)}
			}
		}
		m["workers"] = 1
	}
	if headers := object(s["headers"]); len(headers) > 0 && (protocol == "http" || protocol == "https") {
		m["headers"] = headers
	}
	return m, nil
}
