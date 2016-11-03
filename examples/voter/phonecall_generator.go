/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

package main

import (
	"math"
	"math/rand"
	"strconv"
	"strings"
	"time"
)

var areaCodeStrs = strings.Split("907,205,256,334,251,870,501,479"+
	",480,602,623,928,520,341,764,628,831,925,909,562,661,510,650,949,760"+
	",415,951,209,669,408,559,626,442,530,916,627,714,707,310,323,213,424"+
	",747,818,858,935,619,805,369,720,303,970,719,860,203,959,475,202,302"+
	",689,407,239,850,727,321,754,954,927,352,863,386,904,561,772,786,305"+
	",941,813,478,770,470,404,762,706,678,912,229,808,515,319,563,641,712"+
	",208,217,872,312,773,464,708,224,847,779,815,618,309,331,630,317,765"+
	",574,260,219,812,913,785,316,620,606,859,502,270,504,985,225,318,337"+
	",774,508,339,781,857,617,978,351,413,443,410,301,240,207,517,810,278"+
	",679,313,586,947,248,734,269,989,906,616,231,612,320,651,763,952,218"+
	",507,636,660,975,816,573,314,557,417,769,601,662,228,406,336,252,984"+
	",919,980,910,828,704,701,402,308,603,908,848,732,551,201,862,973,609"+
	",856,575,957,505,775,702,315,518,646,347,212,718,516,917,845,631,716"+
	",585,607,914,216,330,234,567,419,440,380,740,614,283,513,937,918,580"+
	",405,503,541,971,814,717,570,878,835,484,610,267,215,724,412,401,843"+
	",864,803,605,423,865,931,615,901,731,254,325,713,940,817,430,903,806"+
	",737,512,361,210,979,936,409,972,469,214,682,832,281,830,956,432,915"+
	",435,801,385,434,804,757,703,571,276,236,540,802,509,360,564,206,425"+
	",253,715,920,262,414,608,304,307", ",")

var areaCode []int

func init() {
	areaCode = make([]int, len(areaCodeStrs))
	for i, v := range areaCodeStrs {
		areaCode[i], _ = strconv.Atoi(v)
	}
}

type phoneCallGenerator struct {
	contestantCount int32
	votingMap       []int32
}

func newPhoneCallGenerator(contestantCount int) phoneCallGenerator {
	rand.Seed(time.Now().UTC().UnixNano())
	pcg := new(phoneCallGenerator)
	pcg.contestantCount = int32(contestantCount)
	pcg.votingMap = make([]int32, len(areaCode))
	for i := range pcg.votingMap {
		pcg.votingMap[i] = 1
		if rand.Intn(100) >= 30 {
			pcg.votingMap[i] = int32(uint32(math.Sin(float64(i))*float64(contestantCount)) % uint32(contestantCount))
		}
	}
	return *pcg
}

// Receives/generates a simulated voting call
func (pcg phoneCallGenerator) receive() (contestantNumber int32, phoneNumber int64) {
	// For the purpose of a benchmark, issue random voting activity
	// (including invalid votes to demonstrate transaction validationg in the database)

	// Pick a random area code for the originating phone call
	areaCodeIndex := rand.Intn(len(areaCode))

	// Pick a contestant number
	contestantNumber = pcg.votingMap[areaCodeIndex]
	if rand.Intn(2) > 0 {
		contestantNumber = rand.Int31n(pcg.contestantCount) + 1
	}

	//  introduce an invalid contestant every 100 call or so to simulate fraud
	//  and invalid entries (something the transaction validates against)
	if rand.Intn(100) == 0 {
		contestantNumber = 999
	}

	// Build the phone number
	phoneNumber = int64(areaCode[areaCodeIndex])*10000000 + rand.Int63n(10000000)

	// Return the generated phone number
	return contestantNumber, phoneNumber
}
