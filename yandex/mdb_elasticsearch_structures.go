package yandex

import (
	"fmt"

	"github.com/hashicorp/terraform-plugin-sdk/helper/schema"
	"github.com/yandex-cloud/go-genproto/yandex/cloud/mdb/elasticsearch/v1"
)

func parseElasticsearchEnv(e string) (elasticsearch.Cluster_Environment, error) {
	v, ok := elasticsearch.Cluster_Environment_value[e]
	if !ok {
		return 0, fmt.Errorf("value for 'environment' must be one of %s, not `%s`",
			getJoinedKeys(getEnumValueMapKeys(elasticsearch.Cluster_Environment_value)), e)
	}
	return elasticsearch.Cluster_Environment(v), nil
}

func parseElasticsearchHostType(t string) (elasticsearch.Host_Type, error) {
	v, ok := elasticsearch.Host_Type_value[t]
	if !ok {
		return 0, fmt.Errorf("value for 'host.type' must be one of %s, not `%s`",
			getJoinedKeys(getEnumValueMapKeys(elasticsearch.Host_Type_value)), t)
	}
	return elasticsearch.Host_Type(v), nil
}

func expandElasticsearchConfigSpec(d *schema.ResourceData) *elasticsearch.ConfigSpec {
	config := &elasticsearch.ConfigSpec{

		Version: d.Get("config.0.version").(string),

		ElasticsearchSpec: &elasticsearch.ElasticsearchSpec{
			DataNode: &elasticsearch.ElasticsearchSpec_DataNode{
				Resources: &elasticsearch.Resources{
					ResourcePresetId: d.Get("config.0.data_node.0.resources.0.resource_preset_id").(string),
					DiskTypeId:       d.Get("config.0.data_node.0.resources.0.disk_type_id").(string),
					DiskSize:         toBytes(d.Get("config.0.data_node.0.resources.0.disk_size").(int)),
				},
			},
			Plugins: convertStringSet(d.Get("config.0.plugins").(*schema.Set)),
		},
	}

	if _, exist := d.GetOk("config.0.master_node"); exist {
		config.ElasticsearchSpec.MasterNode = &elasticsearch.ElasticsearchSpec_MasterNode{
			Resources: &elasticsearch.Resources{
				ResourcePresetId: d.Get("config.0.master_node.0.resources.0.resource_preset_id").(string),
				DiskTypeId:       d.Get("config.0.master_node.0.resources.0.disk_type_id").(string),
				DiskSize:         toBytes(d.Get("config.0.master_node.0.resources.0.disk_size").(int)),
			},
		}
	}

	return config
}

func flattenElasticsearchClusterConfig(config *elasticsearch.ClusterConfig) []interface{} {
	res := map[string]interface{}{
		"version": config.Version,
		"plugins": config.Elasticsearch.Plugins,
		"data_node": []interface{}{map[string]interface{}{
			"resources": []interface{}{map[string]interface{}{
				"resource_preset_id": config.Elasticsearch.DataNode.Resources.ResourcePresetId,
				"disk_type_id":       config.Elasticsearch.DataNode.Resources.DiskTypeId,
				"disk_size":          toGigabytes(config.Elasticsearch.DataNode.Resources.DiskSize),
			}},
		}},
	}

	if config.Elasticsearch.MasterNode != nil && config.Elasticsearch.MasterNode.Resources != nil {
		res["master_node"] = []interface{}{map[string]interface{}{
			"resources": []interface{}{map[string]interface{}{
				"resource_preset_id": config.Elasticsearch.MasterNode.Resources.ResourcePresetId,
				"disk_type_id":       config.Elasticsearch.MasterNode.Resources.DiskTypeId,
				"disk_size":          toGigabytes(config.Elasticsearch.MasterNode.Resources.DiskSize),
			}},
		}}
	}

	return []interface{}{res}
}

// func flattenElasticsearchUsers(users []*elasticsearch.User, passwords map[string]string) *schema.Set {
// 	result := schema.NewSet(ElasticsearchUserHash, nil)

// 	for _, user := range users {
// 		u := map[string]interface{}{}
// 		u["name"] = user.Name

// 		// if p, ok := passwords[user.Name]; ok {
// 		// 	u["password"] = p
// 		// }
// 		result.Add(u)
// 	}
// 	return result
// }

func expandElasticsearchUser(u map[string]interface{}) *elasticsearch.UserSpec {
	user := &elasticsearch.UserSpec{}

	if v, ok := u["name"]; ok {
		user.Name = v.(string)
	}

	if v, ok := u["password"]; ok {
		user.Password = v.(string)
	}

	return user
}

func expandElasticsearchUserSpecs(d *schema.ResourceData) []*elasticsearch.UserSpec {
	result := []*elasticsearch.UserSpec{}
	users := d.Get("user").(*schema.Set)

	for _, u := range users.List() {
		m := u.(map[string]interface{})

		result = append(result, expandElasticsearchUser(m))
	}

	return result
}

func expandElasticsearchHostSpecs(d *schema.ResourceData) ([]*elasticsearch.HostSpec, error) {
	var result []*elasticsearch.HostSpec
	hosts := d.Get("host").([]interface{})

	for _, v := range hosts {
		config := v.(map[string]interface{})
		host, err := expandElasticsearchHost(config)
		if err != nil {
			return nil, err
		}
		result = append(result, host)
	}

	return result, nil
}

func expandElasticsearchHost(config map[string]interface{}) (*elasticsearch.HostSpec, error) {
	host := &elasticsearch.HostSpec{}
	if v, ok := config["zone"]; ok {
		host.ZoneId = v.(string)
	}

	if v, ok := config["type"]; ok {
		t, err := parseElasticsearchHostType(v.(string))
		if err != nil {
			return nil, err
		}
		host.Type = t
	}

	// if v, ok := config["subnet_id"]; ok {
	// 	host.SubnetId = v.(string)
	// }

	// if v, ok := config["shard_name"]; ok {
	// 	host.ShardName = v.(string)
	// 	if host.Type == elasticsearch.Host_ZOOKEEPER && host.ShardName != "" {
	// 		return nil, fmt.Errorf("ZooKeeper hosts cannot have a 'shard_name'")
	// 	}
	// }

	// if v, ok := config["assign_public_ip"]; ok {
	// 	host.AssignPublicIp = v.(bool)
	// }

	return host, nil
}
