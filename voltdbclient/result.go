package voltdbclient

type VoltResult struct {
}

func NewVoltResult() *VoltResult {
	var vr = new(VoltResult)
	return vr
}

func (vr *VoltResult) LastInsertId() (int64, error) {
	return 0, nil
}

func (vr *VoltResult) RowsAffected() (int64, error) {
	return 0, nil
}
