package show

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
)

type ResourceGroupParam struct {
	framework.ParamBase `use:"show resource-group" desc:"list resource groups in current instance"`
	Name                string `name:"name" default:"" desc:"resource group name to list"`
	Format              string `name:"format" default:"" desc:"output format (default, json)"`
}

func (c *ComponentShow) ResourceGroupCommand(ctx context.Context, p *ResourceGroupParam) (*framework.PresetResultSet, error) {
	rgs, err := common.ListResourceGroups(ctx, c.client, c.metaPath, func(rg *models.ResourceGroup) bool {
		return p.Name == "" || p.Name == rg.GetProto().GetName()
	})
	if err != nil {
		return nil, err
	}

	replicas, err := common.ListReplicas(ctx, c.client, c.metaPath)
	if err != nil {
		return nil, err
	}

	// Group replicas by resource group name.
	replicasByRG := make(map[string][]*models.Replica)
	for _, r := range replicas {
		rgName := r.GetProto().GetResourceGroup()
		replicasByRG[rgName] = append(replicasByRG[rgName], r)
	}

	rs := &ResourceGroups{replicasByRG: replicasByRG}
	rs.SetData(rgs)
	return framework.NewPresetResultSet(rs, framework.NameFormat(p.Format)), nil
}

type ResourceGroups struct {
	framework.ListResultSet[*models.ResourceGroup]
	replicasByRG map[string][]*models.Replica
}

// sqNodes collects unique RwSqNodes from all replicas in the given resource group.
func (rs *ResourceGroups) sqNodes(rgName string) []int64 {
	seen := make(map[int64]struct{})
	for _, r := range rs.replicasByRG[rgName] {
		for _, n := range r.GetProto().GetRwSqNodes() {
			seen[n] = struct{}{}
		}
	}
	if len(seen) == 0 {
		return nil
	}
	nodes := make([]int64, 0, len(seen))
	for n := range seen {
		nodes = append(nodes, n)
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i] < nodes[j] })
	return nodes
}

func (rs *ResourceGroups) PrintAs(format framework.Format) string {
	switch format {
	case framework.FormatDefault, framework.FormatPlain:
		sb := &strings.Builder{}
		for _, info := range rs.Data {
			rg := info.GetProto()
			fmt.Fprintf(sb, "Resource Group Name: %s\tCapacity[Legacy]: %d\tNodes: %v\tLimit: %d\tRequest: %d\n", rg.GetName(), rg.GetCapacity(), rg.GetNodes(), rg.GetConfig().GetLimits().GetNodeNum(), rg.GetConfig().GetRequests().GetNodeNum())
			if sqNodes := rs.sqNodes(rg.GetName()); len(sqNodes) > 0 {
				fmt.Fprintf(sb, "\tSQNodes: %v\n", sqNodes)
			}
		}
		fmt.Fprintf(sb, "--- Total Resource Group(s): %d\n", len(rs.Data))
		return sb.String()
	case framework.FormatJSON:
		return rs.printAsJSON()
	}
	return ""
}

func (rs *ResourceGroups) printAsJSON() string {
	type ResourceGroupJSON struct {
		Name           string  `json:"name"`
		CapacityLegacy int32   `json:"capacity_legacy"`
		Nodes          []int64 `json:"nodes"`
		SQNodes        []int64 `json:"sq_nodes,omitempty"`
		LimitNodeNum   int32   `json:"limit_node_num"`
		RequestNodeNum int32   `json:"request_node_num"`
	}

	type OutputJSON struct {
		ResourceGroups []ResourceGroupJSON `json:"resource_groups"`
		Total          int                 `json:"total"`
	}

	output := OutputJSON{
		ResourceGroups: make([]ResourceGroupJSON, 0, len(rs.Data)),
		Total:          len(rs.Data),
	}

	for _, info := range rs.Data {
		rg := info.GetProto()
		output.ResourceGroups = append(output.ResourceGroups, ResourceGroupJSON{
			Name:           rg.GetName(),
			CapacityLegacy: rg.GetCapacity(),
			Nodes:          rg.GetNodes(),
			SQNodes:        rs.sqNodes(rg.GetName()),
			LimitNodeNum:   rg.GetConfig().GetLimits().GetNodeNum(),
			RequestNodeNum: rg.GetConfig().GetRequests().GetNodeNum(),
		})
	}

	return framework.MarshalJSON(output)
}
