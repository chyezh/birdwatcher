// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package states

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/milvus-io/birdwatcher/common"
	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	stateCommon "github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/birdwatcher/states/kv"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
)

const (
	logicalBits     = 18
	logicalBitsMask = (1 << logicalBits) - 1
)

type ParseTSParam struct {
	framework.ParamBase `use:"parse-ts" desc:"parse hybrid timestamp"`
	args                []string
}

func (p *ParseTSParam) ParseArgs(args []string) error {
	p.args = args
	return nil
}

func (app *ApplicationState) ParseTSCommand(ctx context.Context, p *ParseTSParam) {
	if len(p.args) == 0 {
		fmt.Println("no ts provided")
	}

	for _, arg := range p.args {
		ts, err := strconv.ParseUint(arg, 10, 64)
		if err != nil {
			fmt.Printf("failed to parse ts from %s, err: %s\n", arg, err.Error())
			continue
		}

		t, _ := ParseTS(ts)
		fmt.Printf("Parse ts result, ts:%d, time: %v\n", ts, t)
	}
}

type PrintVerParam struct {
	framework.ParamBase `use:"version" desc:"print version"`
}

func (app *ApplicationState) PrintVersionCommand(ctx context.Context, _ *PrintVerParam) {
	fmt.Println("Birdwatcher Version", common.Version)
}

func ParseTS(ts uint64) (time.Time, uint64) {
	logical := ts & logicalBitsMask
	physical := ts >> logicalBits
	physicalTime := time.Unix(int64(physical/1000), int64(physical)%1000*time.Millisecond.Nanoseconds())
	return physicalTime, logical
}

// ComposeTS returns a timestamp composed of physical part and logical part
func ComposeTS(physical, logical int64) uint64 {
	return uint64((physical << logicalBits) + logical)
}

// listSessions returns all session
func listSessionsByPrefix(cli kv.MetaKV, prefix string) ([]*models.Session, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	_, vals, err := cli.LoadWithPrefix(ctx, prefix)
	if err != nil {
		return nil, err
	}

	sessions := make([]*models.Session, 0, len(vals))
	for _, val := range vals {
		session := &models.Session{}
		err := json.Unmarshal([]byte(val), session)
		if err != nil {
			continue
		}

		sessions = append(sessions, session)
	}
	return sessions, nil
}

// reviseVChannelInfo will revise the datapb.VchannelInfo for upgrade compatibility from 2.0.2
func reviseVChannelInfo(vChannel *datapb.VchannelInfo) {
	removeDuplicateSegmentIDFn := func(ids []int64) []int64 {
		result := make([]int64, 0, len(ids))
		existDict := make(map[int64]bool)
		for _, id := range ids {
			if _, ok := existDict[id]; !ok {
				existDict[id] = true
				result = append(result, id)
			}
		}
		return result
	}

	if vChannel == nil {
		return
	}
	// if the segment infos is not nil(generated by 2.0.2), append the corresponding IDs to segmentIDs
	// and remove the segment infos, remove deplicate ids in case there are some mixed situations
	if len(vChannel.FlushedSegments) > 0 {
		for _, segment := range vChannel.FlushedSegments {
			vChannel.FlushedSegmentIds = append(vChannel.GetFlushedSegmentIds(), segment.GetID())
		}
		vChannel.FlushedSegments = []*datapb.SegmentInfo{}
	}
	vChannel.FlushedSegmentIds = removeDuplicateSegmentIDFn(vChannel.GetFlushedSegmentIds())

	if len(vChannel.UnflushedSegments) > 0 {
		for _, segment := range vChannel.UnflushedSegments {
			vChannel.UnflushedSegmentIds = append(vChannel.GetUnflushedSegmentIds(), segment.GetID())
		}
		vChannel.UnflushedSegments = []*datapb.SegmentInfo{}
	}
	vChannel.UnflushedSegmentIds = removeDuplicateSegmentIDFn(vChannel.GetUnflushedSegmentIds())

	if len(vChannel.DroppedSegments) > 0 {
		for _, segment := range vChannel.DroppedSegments {
			vChannel.DroppedSegmentIds = append(vChannel.GetDroppedSegmentIds(), segment.GetID())
		}
		vChannel.DroppedSegments = []*datapb.SegmentInfo{}
	}
	vChannel.DroppedSegmentIds = removeDuplicateSegmentIDFn(vChannel.GetDroppedSegmentIds())
}

type infoWithCollectionID interface {
	GetCollectionID() int64
	String() string
}

func printInfoWithCollectionID(infos []infoWithCollectionID) {
	var collectionIDs []int64
	collectionMap := make(map[int64][]infoWithCollectionID)
	for _, info := range infos {
		collectionID := info.GetCollectionID()
		collectionIDs = append(collectionIDs, collectionID)
		sliceInfo := collectionMap[collectionID]
		sliceInfo = append(sliceInfo, info)
		collectionMap[collectionID] = sliceInfo
	}

	sort.Slice(collectionIDs, func(i, j int) bool {
		return collectionIDs[i] < collectionIDs[j]
	})

	for _, colID := range collectionIDs {
		sliceInfos := collectionMap[colID]
		for _, info := range sliceInfos {
			fmt.Printf("%s\n", info.String())
		}
	}
}

// getKVPair iterates KV pairs to find specified key.
func getKVPair[T interface {
	GetKey() string
	GetValue() string
}](pairs []T, key string) string {
	for _, pair := range pairs {
		if pair.GetKey() == key {
			return pair.GetValue()
		}
	}
	return ""
}

func pathPartInt64(p string, idx int) (int64, error) {
	part, err := pathPart(p, idx)
	if err != nil {
		return 0, err
	}
	v, err := strconv.ParseInt(part, 10, 64)
	if err != nil {
		return 0, err
	}
	return v, nil
}

func pathPart(p string, idx int) (string, error) {
	parts := strings.Split(p, "/")
	// -1 means last part
	if idx < 0 {
		idx = len(parts) + idx
	}
	if idx < 0 && idx >= len(parts) {
		return "", errors.New("out of index")
	}
	return parts[idx], nil
}

func ListServers(ctx context.Context, cli kv.MetaKV, basePath string, serverName string) ([]*models.Session, error) {
	sessions, err := stateCommon.ListSessions(ctx, cli, basePath)
	if err != nil {
		return nil, err
	}
	targetSessions := make([]*models.Session, 0)
	for _, session := range sessions {
		if session.ServerName == serverName {
			targetSessions = append(targetSessions, session)
		}
	}
	return targetSessions, nil
}
