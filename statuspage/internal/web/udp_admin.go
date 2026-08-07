package web

import "net/http"

func (s *Server) udpAdmin(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/udp-admin" || r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}
	page, err := assets.ReadFile("assets/udp_admin.html")
	if err != nil {
		http.Error(w, "admin UI unavailable", http.StatusInternalServerError)
		return
	}
	s.send(w, r, http.StatusOK, page, "text/html; charset=utf-8", noCache)
}
