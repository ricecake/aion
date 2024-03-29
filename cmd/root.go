/*
Copyright © 2019 Sebastian Green-Husted <geoffcake@gmail.com>

*/
package cmd

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"

	"github.com/gin-gonic/gin"
	"github.com/hashicorp/memberlist"
	homedir "github.com/mitchellh/go-homedir"
	"github.com/pborman/uuid"
	"github.com/robfig/cron/v3"
	"github.com/serialx/hashring"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	bolt "go.etcd.io/bbolt"
)

var cfgFile string

var (
	broadcasts *memberlist.TransmitLimitedQueue
	db         *bolt.DB
	sched      *cron.Cron
	ring       *hashring.HashRing
)

type task struct {
	Name       string
	Code       string
	Definition string
	Command    string
	id         int
}

type ActionType int

const (
	ADD ActionType = iota
	REMOVE
	UPDATE
)

type action struct {
	Task task
	Type ActionType
}

type delegate struct{}

func (d *delegate) NodeMeta(limit int) []byte {
	return []byte{}
}

func (d *delegate) NotifyMsg(b []byte) {
	fmt.Println(string(b))
	var actionMsg action
	jsonErr := json.Unmarshal(b, &actionMsg)
	if jsonErr != nil {
		log.Error(jsonErr)
	}
	switch actionMsg.Type {
	case ADD:
		err := db.Update(func(tx *bolt.Tx) error {
			// Assume bucket exists and has keys
			b, bErr := tx.CreateBucketIfNotExists([]byte("tasks"))
			if bErr != nil {
				return bErr
			}

			if b.Get([]byte(actionMsg.Task.Code)) != nil {
				return nil
			}

			newId, addErr := sched.AddFunc(actionMsg.Task.Definition, func() {
				// grab the memberlist, and then use rendezvous hashing to
				// decide if this node, or another, is the real one that should
				// do the execution of the task
				ringNode, _ := ring.GetNode(actionMsg.Task.Code)
				fmt.Println(actionMsg.Task.Name + " " + actionMsg.Task.Code + " " + ringNode)
			})
			if addErr != nil {
				return addErr
			}
			actionMsg.Task.id = int(newId)

			jsonData, jsonErr := json.Marshal(actionMsg.Task)
			if jsonErr != nil {
				return jsonErr
			}

			return b.Put([]byte(actionMsg.Task.Code), jsonData)
		})
		if err != nil {
			log.Fatal(err)
		}
	default:
		log.Error("Unsupported message type")
	}
}

func (d *delegate) GetBroadcasts(overhead, limit int) [][]byte {
	if broadcasts != nil {
		return broadcasts.GetBroadcasts(overhead, limit)
	} else {
		return [][]byte{}
	}
}

type stateDump struct {
	Tasks []task
}

func (d *delegate) LocalState(join bool) []byte {
	var response stateDump
	dbErr := db.View(func(tx *bolt.Tx) error {
		// Assume bucket exists and has keys
		b := tx.Bucket([]byte("tasks"))

		b.ForEach(func(k, v []byte) error {
			var oldTask task
			jsonErr := json.Unmarshal(v, &oldTask)
			if jsonErr != nil {
				return jsonErr
			}
			response.Tasks = append(response.Tasks, oldTask)
			return nil
		})
		return nil
	})
	if dbErr != nil {
		log.Error(dbErr)
	}

	jsonData, jsonErr := json.Marshal(response)
	if jsonErr != nil {
		log.Error(jsonErr)
	}
	return []byte(jsonData)
}

func (d *delegate) MergeRemoteState(buf []byte, join bool) {
	var response stateDump
	jsonErr := json.Unmarshal(buf, &response)
	if jsonErr != nil {
		log.Error(jsonErr)
	}

	for _, task := range response.Tasks {
		err := db.Update(func(tx *bolt.Tx) error {
			// Assume bucket exists and has keys
			b, bErr := tx.CreateBucketIfNotExists([]byte("tasks"))
			if bErr != nil {
				return bErr
			}

			if b.Get([]byte(task.Code)) != nil {
				return nil
			}

			newId, addErr := sched.AddFunc(task.Definition, func() {
				// grab the memberlist, and then use rendezvous hashing to
				// decide if this node, or another, is the real one that should
				// do the execution of the task
				ringNode, _ := ring.GetNode(task.Code)
				fmt.Println(task.Name + " " + task.Code + " " + ringNode)
			})
			if addErr != nil {
				return addErr
			}
			task.id = int(newId)

			jsonData, jsonErr := json.Marshal(task)
			if jsonErr != nil {
				return jsonErr
			}

			return b.Put([]byte(task.Code), jsonData)
		})
		if err != nil {
			log.Error(err)
		}
	}
}

type eventDelegate struct{}

func (ed *eventDelegate) NotifyJoin(node *memberlist.Node) {
	ring = ring.AddNode(node.String())
	fmt.Println("A node has joined: " + node.String())
}

func (ed *eventDelegate) NotifyLeave(node *memberlist.Node) {
	ring = ring.RemoveNode(node.String())
	fmt.Println("A node has left: " + node.String())
}

func (ed *eventDelegate) NotifyUpdate(node *memberlist.Node) {
	fmt.Println("A node was updated: " + node.String())
}

type broadcast struct {
	msg    []byte
	notify chan<- struct{}
}

func (b *broadcast) Invalidates(other memberlist.Broadcast) bool {
	return false
}

func (b *broadcast) Message() []byte {
	return b.msg
}

func (b *broadcast) Finished() {
	if b.notify != nil {
		close(b.notify)
	}
}

var rootCmd = &cobra.Command{
	Use:   "aion",
	Short: "A brief description of your application",
	Long:  `A longer description`,
	Run: func(cmd *cobra.Command, args []string) {
		var err error
		db, err = bolt.Open(viper.GetString("db.file"), 0600, nil)
		if err != nil {
			log.Fatal(err)
		}
		defer db.Close()

		sched = cron.New()

		dbErr := db.Update(func(tx *bolt.Tx) error {
			// Assume bucket exists and has keys
			b, bErr := tx.CreateBucketIfNotExists([]byte("tasks"))
			if bErr != nil {
				return bErr
			}

			b.ForEach(func(k, v []byte) error {
				var oldTask task
				jsonErr := json.Unmarshal(v, &oldTask)
				if jsonErr != nil {
					return jsonErr
				}

				newId, addErr := sched.AddFunc(oldTask.Definition, func() {
					ringNode, _ := ring.GetNode(oldTask.Code)
					fmt.Println(oldTask.Name + " " + oldTask.Code + " " + ringNode)
				})
				if addErr != nil {
					return addErr
				}
				oldTask.id = int(newId)

				jsonData, jsonErr := json.Marshal(oldTask)
				if jsonErr != nil {
					return jsonErr
				}

				return b.Put([]byte(oldTask.Code), jsonData)
			})
			return nil
		})
		if dbErr != nil {
			log.Fatal(dbErr)
		}

		sched.Start()
		defer sched.Stop()

		hostname, _ := os.Hostname()
		c := memberlist.DefaultLocalConfig()
		c.Events = &eventDelegate{}
		c.Delegate = &delegate{}
		c.BindPort = viper.GetInt("local.gossip")
		c.Name = hostname + "-" + uuid.NewRandom().String()

		ring = hashring.New([]string{})

		m, err := memberlist.Create(c)
		if err != nil {
			log.Fatal(err)
		}

		if viper.IsSet("members") {
			_, err := m.Join(viper.GetStringSlice("members"))
			if err != nil {
				log.Error(err)
			}
		}

		broadcasts = &memberlist.TransmitLimitedQueue{
			NumNodes: func() int {
				return m.NumMembers()
			},
			RetransmitMult: 3,
		}

		node := m.LocalNode()
		log.Printf("Local member %s:%d\n", node.Addr, node.Port)

		for _, mem := range m.Members() {
			fmt.Println(mem.String())
			ring = ring.AddNode(mem.String())
		}

		gin.SetMode("debug")
		r := gin.New()

		r.GET("/info", func(c *gin.Context) {
			var taskList []task
			dbErr := db.Update(func(tx *bolt.Tx) error {
				// Assume bucket exists and has keys
				b, bErr := tx.CreateBucketIfNotExists([]byte("tasks"))
				if bErr != nil {
					return bErr
				}

				b.ForEach(func(k, v []byte) error {
					var oldTask task
					jsonErr := json.Unmarshal(v, &oldTask)
					if jsonErr != nil {
						return jsonErr
					}
					taskList = append(taskList, oldTask)
					return nil
				})
				return nil
			})
			if dbErr != nil {
				log.Error(dbErr)
			}

			c.JSON(200, map[string]interface{}{
				"members": m.Members(),
				"tasks":   taskList,
			})
		})

		r.PUT("/task/:name", func(c *gin.Context) {
			name := c.Param("name")

			newTask := task{
				Name:       name,
				Code:       CompactUUID(),
				Definition: "* * * * *",
				Command:    "Test command",
			}

			err := db.Update(func(tx *bolt.Tx) error {
				// Assume bucket exists and has keys
				b, bErr := tx.CreateBucketIfNotExists([]byte("tasks"))
				if bErr != nil {
					return bErr
				}

				newId, addErr := sched.AddFunc(newTask.Definition, func() {
					// grab the memberlist, and then use rendezvous hashing to
					// decide if this node, or another, is the real one that should
					// do the execution of the task
					ringNode, _ := ring.GetNode(newTask.Code)
					fmt.Println(newTask.Name + " " + newTask.Code + " " + ringNode)
				})
				if addErr != nil {
					return addErr
				}
				newTask.id = int(newId)

				jsonData, jsonErr := json.Marshal(newTask)
				if jsonErr != nil {
					return jsonErr
				}

				return b.Put([]byte(newTask.Code), jsonData)
			})
			if err != nil {
				log.Fatal(err)
			}

			msgData, msgErr := json.Marshal(action{
				Type: ADD,
				Task: newTask,
			})
			if msgErr != nil {
				log.Fatal(msgErr)
			}

			broadcasts.QueueBroadcast(&broadcast{
				msg:    msgData,
				notify: nil,
			})

			c.JSON(200, newTask)
		})

		ginInterface := viper.GetString("local.http.interface")
		ginPort := viper.GetInt("local.http.port")
		ginRunOn := fmt.Sprintf("%s:%d", ginInterface, ginPort)

		r.Run(ginRunOn) // listen and serve on 0.0.0.0:8080
	},
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(initConfig)
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.aion.yaml)")
	rootCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}

func initConfig() {
	if cfgFile != "" {
		viper.SetConfigFile(cfgFile)
	} else {
		home, err := homedir.Dir()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		viper.AddConfigPath(home)
		viper.SetConfigName(".aion")
	}

	viper.AutomaticEnv()

	if err := viper.ReadInConfig(); err == nil {
		fmt.Println("Using config file:", viper.ConfigFileUsed())
	}
}

func CompactUUID() string {
	return base64.RawURLEncoding.EncodeToString([]byte(uuid.NewRandom()))
}
