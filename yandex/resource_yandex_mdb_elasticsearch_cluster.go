package yandex

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/hashicorp/terraform-plugin-sdk/helper/schema"
	"github.com/yandex-cloud/go-genproto/yandex/cloud/mdb/elasticsearch/v1"
	"google.golang.org/genproto/protobuf/field_mask"
)

const (
	yandexMDBElasticsearchClusterCreateTimeout = 30 * time.Minute
	yandexMDBElasticsearchClusterDeleteTimeout = 15 * time.Minute
	yandexMDBElasticsearchClusterUpdateTimeout = 60 * time.Minute
)

func resourceYandexMDBElasticsearchCluster() *schema.Resource {
	return &schema.Resource{

		Create: resourceYandexMDBElasticsearchClusterCreate,
		Read:   resourceYandexMDBElasticsearchClusterRead,
		Update: resourceYandexMDBElasticsearchClusterUpdate,
		Delete: resourceYandexMDBElasticsearchClusterDelete,
		Importer: &schema.ResourceImporter{
			State: schema.ImportStatePassthrough,
		},

		Timeouts: &schema.ResourceTimeout{
			Create: schema.DefaultTimeout(yandexMDBElasticsearchClusterCreateTimeout),
			Update: schema.DefaultTimeout(yandexMDBElasticsearchClusterUpdateTimeout),
			Delete: schema.DefaultTimeout(yandexMDBElasticsearchClusterDeleteTimeout),
		},

		SchemaVersion: 0,

		Schema: map[string]*schema.Schema{

			// Name of the Elasticsearch cluster. The name must be unique within the folder.
			"name": {
				Type:     schema.TypeString,
				Required: true,
			},

			// Description of the Elasticsearch cluster.
			"description": {
				Type:     schema.TypeString,
				Optional: true,
			},

			// Custom labels for the Elasticsearch cluster as `key:value` pairs.
			"labels": {
				Type:     schema.TypeMap,
				Optional: true,
				Elem:     &schema.Schema{Type: schema.TypeString},
				Set:      schema.HashString,
			},

			// ID of the folder that the Elasticsearch cluster belongs to.
			"folder_id": {
				Type:     schema.TypeString,
				Optional: true,
				Computed: true,
				ForceNew: true, // TODO impl move cluster
			},

			// Deployment environment of the Elasticsearch cluster.
			"environment": {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
			},

			// ID of the network that the cluster belongs to.
			"network_id": {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
			},

			// Configuration of the Elasticsearch cluster.
			"config": {
				Type:     schema.TypeList,
				Required: true,
				MaxItems: 1,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"version": {
							Type:     schema.TypeString,
							Required: true,
							ForceNew: true,
						},

						"data_node": {
							Type:     schema.TypeList,
							Required: true,
							MaxItems: 1,
							Elem: &schema.Resource{
								Schema: map[string]*schema.Schema{
									"resources": {
										Type:     schema.TypeList,
										MaxItems: 1,
										Required: true,
										Elem: &schema.Resource{
											Schema: map[string]*schema.Schema{
												"resource_preset_id": {
													Type:     schema.TypeString,
													Required: true,
												},
												"disk_size": {
													Type:     schema.TypeInt,
													Required: true,
												},
												"disk_type_id": {
													Type:     schema.TypeString,
													Required: true,
												},
											},
										},
									},
								},
							},
						},

						"master_node": {
							Type:     schema.TypeList,
							Optional: true,
							MaxItems: 1,
							Elem: &schema.Resource{
								Schema: map[string]*schema.Schema{
									"resources": {
										Type:     schema.TypeList,
										MaxItems: 1,
										Required: true,
										Elem: &schema.Resource{
											Schema: map[string]*schema.Schema{
												"resource_preset_id": {
													Type:     schema.TypeString,
													Required: true,
												},
												"disk_size": {
													Type:     schema.TypeInt,
													Required: true,
												},
												"disk_type_id": {
													Type:     schema.TypeString,
													Required: true,
												},
											},
										},
									},
								},
							},
						}, // masternode

						"plugins": {
							Type:     schema.TypeSet,
							Elem:     &schema.Schema{Type: schema.TypeString},
							Set:      schema.HashString,
							Optional: true,
						},
					},
				},
			},

			// User security groups
			"security_group_ids": {
				Type:     schema.TypeSet,
				Elem:     &schema.Schema{Type: schema.TypeString},
				Set:      schema.HashString,
				Optional: true,
			},

			// Aggregated cluster health.
			"health": {
				Type:     schema.TypeString,
				Computed: true,
			},

			// Current state of the cluster.
			"status": {
				Type:     schema.TypeString,
				Computed: true,
			},

			// Creation timestamp.
			"created_at": {
				Type:     schema.TypeString,
				Computed: true,
			},

			"user": {
				Type:     schema.TypeSet,
				Required: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"name": {
							Type:     schema.TypeString,
							Required: true,
						},
						"password": {
							Type:      schema.TypeString,
							Required:  true,
							Sensitive: true,
						},
					},
				},
			},

			"host": {
				Type:     schema.TypeList,
				MinItems: 1,
				Required: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"fqdn": {
							Type:     schema.TypeString,
							Computed: true,
						},
						"zone": {
							Type:     schema.TypeString,
							Required: true,
						},
						"type": {
							Type:         schema.TypeString,
							Required:     true,
							ValidateFunc: validateParsableValue(parseElasticsearchHostType),
						},
						// "assign_public_ip": {
						// 	Type:     schema.TypeBool,
						// 	Optional: true,
						// 	Default:  false,
						// },
						// "subnet_id": {
						// 	Type:     schema.TypeString,
						// 	Optional: true,
						// 	Computed: true,
						// },
						// "shard_name": {
						// 	Type:         schema.TypeString,
						// 	Optional:     true,
						// 	Computed:     true,
						// 	ValidateFunc: validation.NoZeroValues,
						// },
					},
				},
			},

			// TODO monitoring links
		},
	}
}

func resourceYandexMDBElasticsearchClusterRead(d *schema.ResourceData, meta interface{}) error {
	config := meta.(*Config)

	ctx, cancel := config.ContextWithTimeout(d.Timeout(schema.TimeoutRead))
	defer cancel()

	cluster, err := config.sdk.MDB().Elasticsearch().Cluster().Get(ctx, &elasticsearch.GetClusterRequest{
		ClusterId: d.Id(),
	})
	if err != nil {
		return handleNotFoundError(err, d, fmt.Sprintf("Cluster %q", d.Id()))
	}

	createdAt, err := getTimestamp(cluster.CreatedAt)
	if err != nil {
		return err
	}

	d.Set("created_at", createdAt)
	d.Set("health", cluster.GetHealth().String())
	d.Set("status", cluster.GetStatus().String())

	d.Set("folder_id", cluster.GetFolderId())
	d.Set("environment", cluster.GetEnvironment().String())
	d.Set("network_id", cluster.GetNetworkId())

	d.Set("name", cluster.GetName())
	d.Set("description", cluster.GetDescription())

	if err := d.Set("labels", cluster.GetLabels()); err != nil {
		return err
	}

	clusterConfig := flattenElasticsearchClusterConfig(cluster.Config)

	if err := d.Set("config", clusterConfig); err != nil {
		return err
	}

	if err := d.Set("security_group_ids", cluster.SecurityGroupIds); err != nil {
		return err
	}

	// TODO users, hosts

	return nil
}

func resourceYandexMDBElasticsearchClusterCreate(d *schema.ResourceData, meta interface{}) error {
	config := meta.(*Config)

	req, err := prepareCreateElasticsearchRequest(d, config)

	if err != nil {
		return err
	}

	ctx, cancel := config.ContextWithTimeout(d.Timeout(schema.TimeoutCreate))
	defer cancel()

	op, err := config.sdk.WrapOperation(config.sdk.MDB().Elasticsearch().Cluster().Create(ctx, req))
	if err != nil {
		return fmt.Errorf("Error while requesting API to create Elasticsearch Cluster: %s", err)
	}

	protoMetadata, err := op.Metadata()
	if err != nil {
		return fmt.Errorf("Error while get Elasticsearch Cluster create operation metadata: %s", err)
	}

	md, ok := protoMetadata.(*elasticsearch.CreateClusterMetadata)
	if !ok {
		return fmt.Errorf("Could not get Elasticsearch Cluster ID from create operation metadata")
	}

	d.SetId(md.ClusterId)

	err = op.Wait(ctx)
	if err != nil {
		return fmt.Errorf("Error while waiting for operation to create Elasticsearch Cluster: %s", err)
	}

	if _, err := op.Response(); err != nil {
		return fmt.Errorf("Elasticsearch Cluster creation failed: %s", err)
	}

	return resourceYandexMDBElasticsearchClusterRead(d, meta)
}

func prepareCreateElasticsearchRequest(d *schema.ResourceData, meta *Config) (*elasticsearch.CreateClusterRequest, error) {
	labels, err := expandLabels(d.Get("labels"))
	if err != nil {
		return nil, fmt.Errorf("error while expanding labels on Elasticsearch Cluster create: %s", err)
	}

	folderID, err := getFolderID(d, meta)
	if err != nil {
		return nil, fmt.Errorf("Error getting folder ID while creating Elasticsearch Cluster: %s", err)
	}

	e := d.Get("environment").(string)
	env, err := parseElasticsearchEnv(e)
	if err != nil {
		return nil, fmt.Errorf("Error resolving environment while creating Elasticsearch Cluster: %s", err)
	}

	securityGroupIds := expandSecurityGroupIds(d.Get("security_group_ids"))

	config := expandElasticsearchConfigSpec(d)

	users := expandElasticsearchUserSpecs(d)

	hosts, err := expandElasticsearchHostSpecs(d)
	if err != nil {
		return nil, fmt.Errorf("Error while expanding hosts on Elasticsearch Cluster create: %s", err)
	}

	req := &elasticsearch.CreateClusterRequest{
		Name:        d.Get("name").(string),
		Description: d.Get("description").(string),
		Labels:      labels,

		FolderId:    folderID,
		Environment: env,
		NetworkId:   d.Get("network_id").(string),

		ConfigSpec: config,

		UserSpecs: users,

		HostSpecs: hosts,

		SecurityGroupIds: securityGroupIds,
	}

	return req, nil
}

func resourceYandexMDBElasticsearchClusterDelete(d *schema.ResourceData, meta interface{}) error {
	config := meta.(*Config)

	log.Printf("[DEBUG] Deleting Elasticsearch Cluster %q", d.Id())

	req := &elasticsearch.DeleteClusterRequest{
		ClusterId: d.Id(),
	}

	ctx, cancel := config.ContextWithTimeout(d.Timeout(schema.TimeoutDelete))
	defer cancel()

	op, err := config.sdk.WrapOperation(config.sdk.MDB().Elasticsearch().Cluster().Delete(ctx, req))
	if err != nil {
		return handleNotFoundError(err, d, fmt.Sprintf("Elasticsearch Cluster %q", d.Id()))
	}

	err = op.Wait(ctx)
	if err != nil {
		return err
	}

	_, err = op.Response()
	if err != nil {
		return err
	}

	log.Printf("[DEBUG] Finished deleting Elasticsearch Cluster %q", d.Id())

	return nil
}

func resourceYandexMDBElasticsearchClusterUpdate(d *schema.ResourceData, meta interface{}) error {
	d.Partial(true)

	if err := updateElasticsearchClusterParams(d, meta); err != nil {
		return err
	}

	// TODO users, host

	d.Partial(false)
	return resourceYandexMDBElasticsearchClusterRead(d, meta)
}

func updateElasticsearchClusterParams(d *schema.ResourceData, meta interface{}) error {
	req := &elasticsearch.UpdateClusterRequest{
		ClusterId: d.Id(),
		UpdateMask: &field_mask.FieldMask{
			Paths: make([]string, 0, 16),
		},
	}
	changed := make([]string, 0, 16)

	if d.HasChange("description") {
		req.Description = d.Get("description").(string)
		req.UpdateMask.Paths = append(req.UpdateMask.Paths, "description")

		changed = append(changed, "description")
	}

	if d.HasChange("name") {
		req.Name = d.Get("name").(string)
		req.UpdateMask.Paths = append(req.UpdateMask.Paths, "name")

		changed = append(changed, "name")
	}

	if d.HasChange("labels") {
		labelsProp, err := expandLabels(d.Get("labels"))
		if err != nil {
			return err
		}

		req.Labels = labelsProp
		req.UpdateMask.Paths = append(req.UpdateMask.Paths, "labels")

		changed = append(changed, "labels")
	}

	if d.HasChange("security_group_ids") {
		securityGroupIds := expandSecurityGroupIds(d.Get("security_group_ids"))

		req.SecurityGroupIds = securityGroupIds
		req.UpdateMask.Paths = append(req.UpdateMask.Paths, "security_group_ids")

		changed = append(changed, "security_group_ids")
	}

	// TODO no way update env, net

	// TODO  folder, conf

	// if d.HasChange("config") {
	// 	config := expandElasticsearchConfigSpec(d)
	// 	req.ConfigSpec = config
	// 	req.UpdateMask.Paths = append(req.UpdateMask.Paths, "config")

	// 	changed = append(changed, "config")
	// }

	if len(changed) == 0 {
		return nil // nothing to update
	}

	err := makeElasticsearchClusterUpdateRequest(req, d, meta)
	if err != nil {
		return err
	}

	for i := range changed {
		d.SetPartial(changed[i])
	}
	return nil
}

func makeElasticsearchClusterUpdateRequest(req *elasticsearch.UpdateClusterRequest, d *schema.ResourceData, meta interface{}) error {
	config := meta.(*Config)

	ctx, cancel := context.WithTimeout(context.Background(), d.Timeout(schema.TimeoutUpdate))
	defer cancel()

	op, err := config.sdk.WrapOperation(config.sdk.MDB().Elasticsearch().Cluster().Update(ctx, req))
	if err != nil {
		return fmt.Errorf("Error while requesting API to update Elasticsearch Cluster %q: %s", d.Id(), err)
	}

	err = op.Wait(ctx)
	if err != nil {
		return fmt.Errorf("Error updating Elasticsearch Cluster %q: %s", d.Id(), err)
	}
	return nil
}
