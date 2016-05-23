package voltdbclient

type VoltTableRow struct {
	vt *VoltTable
}

func NewVoltTableRow(table *VoltTable) (*VoltTableRow) {
	var vtr = new(VoltTableRow)
	vtr.vt = table
	return vtr
}

func (vtr *VoltTableRow) AdvanceRow() bool {
	return vtr.vt.AdvanceRow()
}

func (vtr *VoltTableRow) AdvanceToRow(rowIndex int32) bool {
	return vtr.vt.AdvanceToRow(rowIndex)
}

func (vtr *VoltTableRow) GetString(colIndex int16) (string,  error) {
	return vtr.vt.GetString(vtr.vt.rowIndex, colIndex)
}
