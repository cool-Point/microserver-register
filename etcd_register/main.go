package main

import (
	"context"
	"fmt"
	"time"

	"go.etcd.io/etcd/clientv3"
)

//创建租约注册服务
type ServiceReg struct {
	client         *clientv3.Client
	lease          clientv3.Lease
	leaseResp      *clientv3.LeaseGrantResponse
	cancelfunc     context.CancelFunc
	keeppAliveChan <-chan *clientv3.LeaseKeepAliveResponse
	key            string
}

func NewServiceReg(addr []string, timeNum int64) (*ServiceReg, error) {
	conf := clientv3.Config{
		Endpoints:   addr,
		DialTimeout: 5 * time.Second,
	}
	var (
		client *clientv3.Client
	)
	if clientTem, err := clientv3.New(conf); err == nil {
		client = clientTem
	} else {
		return nil, err
	}
	ser := &ServiceReg{
		client: client,
	}
	if err := ser.setLease(timeNum); err != nil {
		return nil, err
	}
	go ser.ListenLeaseRespChan()
	return ser, nil
}

//设置租约
func (s *ServiceReg) setLease(timeNum int64) error {
	lease := clientv3.NewLease(s.client)

	//设置租约时间
	leaseResp, err := lease.Grant(context.TODO(), timeNum)
	if err != nil {
		return err
	}

	//设置续租
	ctx, cancelfunc := context.WithCancel(context.TODO())
	leaseRespChan, err := lease.KeepAlive(ctx, leaseResp.ID)
	if err != nil {
		return err
	}
	s.lease = lease
	s.leaseResp = leaseResp
	s.cancelfunc = cancelfunc
	s.keeppAliveChan = leaseRespChan
	return nil
}

//监听 续租情况
func (s *ServiceReg) ListenLeaseRespChan() {
	for {
		select {
		case leaseKeepResp := <-s.keeppAliveChan:
			if leaseKeepResp == nil {
				fmt.Printf("已经关闭续租功能\n")
			} else {
				fmt.Printf("续租成功\n")
			}
		}
	}
}

//通过租约 注册服务
func (s *ServiceReg) PutService(key, val string) error {
	kv := clientv3.NewKV(s.client)
	_, err := kv.Put(context.TODO(), key, val, clientv3.WithLease(s.leaseResp.ID))
	return err
}

//撤销租约
func (s *ServiceReg) RevokeLease() error {
	s.cancelfunc()
	time.Sleep(2 * time.Second)
	_, err := s.lease.Revoke(context.TODO(), s.leaseResp.ID)
	return err
}

func main() {
	ser, _ := NewServiceReg([]string{"127.0.0.1:2379"}, 5)
	ser.PutService("/node/micro", "127.0.0.1:8000")
	select {}
}
//https://www.jianshu.com/p/7c0d23c818a5