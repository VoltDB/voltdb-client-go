/* This file is part of VoltDB.
 * Copyright (C) 2025 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package wire

import (
	"time"
)

const (
	DayShift   = 0
	MonthShift = 8
	YearShift  = 16

	OneByteMask = 0xFF
	TwoByteMask = 0xFFFF
)

func EncodeDate(t time.Time) int32 {
	year, month, day := t.Date()

	return (int32(year) << YearShift) |
		(int32(month) << MonthShift) |
		(int32(day) << DayShift)
}

func DecodeDate(encoded int32) time.Time {
	day := int((encoded >> DayShift) & OneByteMask)
	month := int((encoded >> MonthShift) & OneByteMask)
	year := int((encoded >> YearShift) & TwoByteMask)

	return time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.Local)
}
