package voltdbclient

type hashinater struct {
}

func newHashinater() *hashinater {
	return new(hashinater)
}

func (h *hashinater) getConn(pi *procedureInvocation) *nodeConn {
	return nil
}
