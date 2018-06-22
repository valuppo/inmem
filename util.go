package inmem

import (
	"log"
	"net"
)

// getLocalIP : get local IP for current machine
func getLocalIP() string {
	addrs, errNet := net.InterfaceAddrs()
	var localIP string
	if errNet != nil {
		log.Println("Oops: " + errNet.Error() + "\n")
		return ""
	}

	for _, a := range addrs {
		if ipnet, ok := a.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				localIP = ipnet.IP.String()
			}
		}
	}

	return localIP
}
