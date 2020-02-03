package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"
)

type ClientDis struct {
	client     *clientv3.Client
	serverList map[string]string
	lock       sync.Mutex
}

func NewClientDis(addr []string) (*ClientDis, error) {
	conf := clientv3.Config{
		Endpoints:   addr,
		DialTimeout: 5 * time.Second,
	}
	if client, err := clientv3.New(conf); err == nil {
		return &ClientDis{
			client:     client,
			serverList: make(map[string]string),
		}, nil
	} else {
		return nil, err
	}
}

func (c *ClientDis) GetService(prefix string) ([]string, error) {
	resp, err := c.client.Get(context.Background(), prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	addrs := c.extractAddr(resp)

	go c.watcher(prefix)
	return addrs, nil
}

func (c *ClientDis) watcher(prefix string) {
	rch := c.client.Watch(context.Background(), prefix, clientv3.WithPrefix())
	for wresp := range rch {
		for _, ev := range wresp.Events {
			switch ev.Type {
			case mvccpb.PUT:
				c.SetServiceList(string(ev.Kv.Key), string(ev.Kv.Value))
			case mvccpb.DELETE:
				c.DelServiceList(string(ev.Kv.Key))
			}
		}
	}
}

func (c *ClientDis) extractAddr(resp *clientv3.GetResponse) []string {
	addrs := make([]string, 0)
	if resp == nil || resp.Kvs == nil {
		return addrs
	}
	for i := range resp.Kvs {
		if v := resp.Kvs[i].Value; v != nil {
			c.SetServiceList(string(resp.Kvs[i].Key), string(resp.Kvs[i].Value))
			addrs = append(addrs, string(v))
		}
	}
	return addrs
}

func (c *ClientDis) SetServiceList(key, val string) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.serverList[key] = string(val)
	log.Println("set data key:", key, "val:", val)
}

func (c *ClientDis) DelServiceList(key string) {
	c.lock.Lock()
	defer c.lock.Unlock()
	delete(c.serverList, key)
	log.Println("del data key:", key)
}

func (c *ClientDis) SerList2Array() []string {
	c.lock.Lock()
	defer c.lock.Lock()
	addrs := make([]string, 0)

	for _, v := range c.serverList {
		addrs = append(addrs, v)
	}
	return addrs
}

func main() {
	cli, _ := NewClientDis([]string{"127.0.0.1:2379"})
	cli.GetService("/node")
	fmt.Println(cli.serverList)
	select {}
}
