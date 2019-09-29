/*
Copyright © 2019 Sebastian Green-Husted <geoffcake@gmail.com>

*/
package cmd

import (
	"fmt"
	"os"

	"github.com/hashicorp/memberlist"
	homedir "github.com/mitchellh/go-homedir"
	"github.com/pborman/uuid"
	"github.com/robfig/cron/v3"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.etcd.io/bbolt"
)

var cfgFile string

var (
	broadcasts *memberlist.TransmitLimitedQueue
)

type delegate struct{}

func (d *delegate) NodeMeta(limit int) []byte {
	return []byte{}
}

func (d *delegate) NotifyMsg(b []byte) {
}

func (d *delegate) GetBroadcasts(overhead, limit int) [][]byte {
	return broadcasts.GetBroadcasts(overhead, limit)
}

func (d *delegate) LocalState(join bool) []byte {
	return []byte{}
}

func (d *delegate) MergeRemoteState(buf []byte, join bool) {
}

type eventDelegate struct{}

func (ed *eventDelegate) NotifyJoin(node *memberlist.Node) {
	fmt.Println("A node has joined: " + node.String())
}

func (ed *eventDelegate) NotifyLeave(node *memberlist.Node) {
	fmt.Println("A node has left: " + node.String())
}

func (ed *eventDelegate) NotifyUpdate(node *memberlist.Node) {
	fmt.Println("A node was updated: " + node.String())
}

var rootCmd = &cobra.Command{
	Use:   "aion",
	Short: "A brief description of your application",
	Long:  `A longer description`,
	Run: func(cmd *cobra.Command, args []string) {
		db, err := bbolt.Open(viper.GetString("./db.file"), 0600, nil)
		if err != nil {
			log.Fatal(err)
		}
		defer db.Close()

		sched := cron.New()
		sched.Start()
		defer sched.Stop()

		hostname, _ := os.Hostname()
		c := memberlist.DefaultLocalConfig()
		c.Events = &eventDelegate{}
		c.Delegate = &delegate{}
		c.BindPort = 0
		c.Name = hostname + "-" + uuid.NewUUID().String()
		m, err := memberlist.Create(c)
		if err != nil {
			log.Fatal(err)
		}
		if viper.IsSet("members") {
			_, err := m.Join(viper.GetStringSlice("members"))
			if err != nil {
				log.Fatal(err)
			}
		}
		broadcasts = &memberlist.TransmitLimitedQueue{
			NumNodes: func() int {
				return m.NumMembers()
			},
			RetransmitMult: 3,
		}
		node := m.LocalNode()
		fmt.Printf("Local member %s:%d\n", node.Addr, node.Port)
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
